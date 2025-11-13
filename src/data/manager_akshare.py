import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Sequence

import akshare as ak
import pandas as pd
import pytz
from pymongo import ASCENDING, MongoClient, errors

from ..utils.config_loader import load_config

BEIJING_TZ = pytz.timezone("Asia/Shanghai")
TRADING_DAYS = {0, 1, 2, 3, 4}
TRADING_PERIODS = [("09:30", "11:30"), ("13:00", "15:00")]

logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)


class TimeframeAggregator:
    """合成固定周期（例如 15m/60m）的 K 线。"""

    def __init__(self, timeframe: str, symbol: str, collection):
        self.timeframe = timeframe
        self.symbol = symbol
        self.collection = collection
        self.current_bar_start: Optional[datetime] = None
        self.open_price: Optional[float] = None
        self.high_price: Optional[float] = None
        self.low_price: Optional[float] = None
        self.close_price: Optional[float] = None
        self.volume: float = 0.0

    def _bar_start(self, dt: datetime) -> datetime:
        if not self.timeframe.endswith("m"):
            raise ValueError(f"暂不支持的时间粒度: {self.timeframe}")
        minutes = int(self.timeframe.rstrip("m"))
        total_minutes = dt.hour * 60 + dt.minute
        start_minutes = (total_minutes // minutes) * minutes
        hour = start_minutes // 60
        minute = start_minutes % 60
        return dt.replace(hour=hour, minute=minute, second=0, microsecond=0)

    def _open_bar(self, bar_start: datetime, price: float, vol_increment: float) -> None:
        self.current_bar_start = bar_start
        self.open_price = price
        self.high_price = price
        self.low_price = price
        self.close_price = price
        self.volume = max(vol_increment, 0.0)

    def _save_current_bar(self) -> None:
        if self.current_bar_start is None:
            return
        doc = {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "datetime": self.current_bar_start,
            "open": self.open_price,
            "high": self.high_price,
            "low": self.low_price,
            "close": self.close_price,
            "volume": self.volume,
        }
        self.collection.update_one(
            {"symbol": self.symbol, "timeframe": self.timeframe, "datetime": self.current_bar_start},
            {"$set": doc},
            upsert=True,
        )

    def update_bar(self, dt: datetime, price: float, vol_increment: float) -> None:
        bar_start = self._bar_start(dt)
        if self.current_bar_start is None:
            self._open_bar(bar_start, price, vol_increment)
            return
        if bar_start == self.current_bar_start:
            self.high_price = max(self.high_price, price)
            self.low_price = min(self.low_price, price)
            self.close_price = price
            self.volume += max(vol_increment, 0.0)
        else:
            self._save_current_bar()
            self._open_bar(bar_start, price, vol_increment)

    def flush(self) -> None:
        self._save_current_bar()
        self.current_bar_start = None
        self.open_price = None
        self.high_price = None
        self.low_price = None
        self.close_price = None
        self.volume = 0.0


class AkshareRealtimeManager:
    """
    使用 Akshare spot API 获取实时行情，合成指定周期的 K 线，并写入 MongoDB。
    - 若数据库为空，可搭配 Baostock 历史数据，实现“历史 + 当日实时”的补齐。
    """

    def __init__(self, config_path: str = "config.yaml") -> None:
        self.config = load_config(config_path)
        mongo_uri = self.config.get("mongodb", {}).get("uri", "mongodb://localhost:27017/")
        ak_cfg = self.config.get("akshare", {})

        self.client = MongoClient(ak_cfg.get("uri", mongo_uri))
        self.db = self.client[ak_cfg.get("db", "akshare_realtime")]
        self.kline_collection = self.db[ak_cfg.get("kline", "kline")]
        self.daily_collection = self.db[ak_cfg.get("daily", "daily")]
        self.sleep_seconds = float(ak_cfg.get("sleep_seconds", 5))
        self.timeframes: Sequence[str] = tuple(ak_cfg.get("timeframes", ["15m", "60m"]))

        self.symbols = self._load_symbols(ak_cfg.get("symbols"))
        if not self.symbols:
            raise ValueError("AkshareRealtimeManager: symbols 列表为空，请先在配置或 stock_basic 中提供股票列表。")

        self.aggregators: Dict[str, Dict[str, TimeframeAggregator]] = {
            timeframe: {symbol: TimeframeAggregator(timeframe, symbol, self.kline_collection) for symbol in self.symbols}
            for timeframe in self.timeframes
        }
        self.volume_cache: Dict[str, float] = {symbol: 0.0 for symbol in self.symbols}
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        self.kline_collection.create_index(
            [("symbol", ASCENDING), ("timeframe", ASCENDING), ("datetime", ASCENDING)],
            unique=True,
            name="symbol_timeframe_datetime_idx",
        )
        self.daily_collection.create_index(
            [("symbol", ASCENDING), ("date", ASCENDING)],
            unique=True,
            name="symbol_date_idx",
        )

    def _normalize_symbol(self, code: str) -> Optional[str]:
        if not code:
            return None
        if "." in code:
            code = code.split(".")[-1]
        return code.strip()

    def _load_symbols(self, configured: Optional[Sequence[str]]) -> List[str]:
        if configured:
            return [self._normalize_symbol(code) for code in configured if self._normalize_symbol(code)]

        baostock_cfg = self.config.get("baostock", {})
        if not baostock_cfg:
            return []

        basic_collection = self.client[baostock_cfg["db"]][baostock_cfg["basic"]]
        codes = [self._normalize_symbol(doc["code"]) for doc in basic_collection.find({}, {"code": 1})]
        filtered = [code for code in codes if code]
        if not filtered:
            logger.warning("在 stock_basic 中未找到可用代码，将回退到示例列表。")
            return ["000001", "000002", "600519"]
        return filtered

    def _is_trading_time(self, now: datetime) -> bool:
        if now.weekday() not in TRADING_DAYS:
            return False
        current = now.strftime("%H:%M")
        return any(start <= current < end for start, end in TRADING_PERIODS)

    def _fetch_quotes(self) -> pd.DataFrame:
        df_all = ak.stock_zh_a_spot_em()
        df_selected = df_all[df_all["代码"].isin(self.symbols)].copy()
        return df_selected

    def _volume_increment(self, symbol: str, total_volume: float) -> float:
        if pd.isna(total_volume):
            return 0.0
        previous = self.volume_cache.get(symbol, 0.0)
        increment = max(total_volume - previous, 0.0)
        self.volume_cache[symbol] = total_volume
        return increment

    def _process_quotes(self, df: pd.DataFrame, now: datetime, force_flush: bool) -> None:
        for _, row in df.iterrows():
            symbol = row["代码"]
            last_price = float(row["最新价"]) if not pd.isna(row["最新价"]) else None
            if last_price is None:
                continue
            vol_increment = self._volume_increment(symbol, row["成交量"])
            for timeframe in self.timeframes:
                aggregator = self.aggregators[timeframe][symbol]
                aggregator.update_bar(now, last_price, vol_increment)

        trade_date = now.strftime("%Y-%m-%d")
        for _, row in df.iterrows():
            self._save_daily_snapshot(row, trade_date)

        if force_flush:
            self._flush_aggregators()

    def _save_daily_snapshot(self, row: pd.Series, trade_date: str) -> None:
        symbol = row["代码"]
        try:
            doc = {
                "symbol": symbol,
                "date": trade_date,
                "latest_price": float(row["最新价"]) if not pd.isna(row["最新价"]) else None,
                "high": float(row["最高"]) if not pd.isna(row["最高"]) else None,
                "low": float(row["最低"]) if not pd.isna(row["最低"]) else None,
                "volume": float(row["成交量"]) if not pd.isna(row["成交量"]) else 0.0,
            }
            self.daily_collection.update_one(
                {"symbol": symbol, "date": trade_date},
                {"$set": doc},
                upsert=True,
            )
        except errors.PyMongoError as exc:
            logger.error("写入 Akshare 日线快照失败 %s: %s", symbol, exc)

    def _flush_aggregators(self) -> None:
        for timeframe_dict in self.aggregators.values():
            for aggregator in timeframe_dict.values():
                aggregator.flush()

    def sync_once(self, ignore_trading_window: bool = False, force_flush: bool = False) -> None:
        """
        拉取一次实时行情，可在日终/手动脚本中调用，force_flush=True 可将当下正在构建的 bar 立即落表。
        """
        now = datetime.now(BEIJING_TZ)
        if not ignore_trading_window and not self._is_trading_time(now):
            logger.info("当前 %s 不在交易时段，跳过 Akshare 实时拉取。", now.strftime("%Y-%m-%d %H:%M:%S"))
            return
        try:
            quotes = self._fetch_quotes()
        except Exception as exc:  # noqa: BLE001
            logger.error("获取 Akshare 实时行情失败: %s", exc)
            return

        if quotes.empty:
            logger.warning("Akshare 返回空数据，检查行情接口或 symbol 列表。")
            return

        self._process_quotes(quotes, now, force_flush=force_flush)
        logger.info("Akshare 实时拉取完成，处理 %d 条记录。", len(quotes))

    def run_loop(self, iterations: Optional[int] = None, ignore_trading_window: bool = False) -> None:
        """
        持续运行的实时采集入口，可用于盘中监控。iterations 用于测试/调试时限定循环次数。
        """
        processed = 0
        try:
            while True:
                self.sync_once(ignore_trading_window=ignore_trading_window, force_flush=False)
                processed += 1
                if iterations and processed >= iterations:
                    break
                time.sleep(self.sleep_seconds)
        except KeyboardInterrupt:
            logger.info("手动终止 Akshare 实时采集。")

    def close(self) -> None:
        self._flush_aggregators()
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
