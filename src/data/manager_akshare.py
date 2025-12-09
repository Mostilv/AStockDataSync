import logging
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence
import socket
import requests
from requests.packages.urllib3.util import connection

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
    """合成固定周期（例如 5m）的 K 线。"""

    def __init__(
        self,
        timeframe: str,
        symbol: str,
        collection,
        source_tag: str = "akshare",
        temp_flag: bool = True,
        on_save: Optional[Callable[[Dict[str, Any]], None]] = None,
    ):
        self.timeframe = timeframe
        self.symbol = symbol
        self.collection = collection
        self.source_tag = source_tag
        self.temporary = temp_flag
        self.on_save = on_save
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
            "source": self.source_tag,
            "temporary": self.temporary,
        }
        self.collection.update_one(
            {"symbol": self.symbol, "timeframe": self.timeframe, "datetime": self.current_bar_start},
            {"$set": doc},
            upsert=True,
        )
        if self.on_save:
            self.on_save(doc)

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

        if ak_cfg.get("force_ipv4", False):
            # Avoid IPv6 routes that may be blocked by some ISPs/corp networks.
            connection.allowed_gai_family = lambda: socket.AF_INET

        self.client = MongoClient(ak_cfg.get("uri", mongo_uri))
        self.db = self.client[ak_cfg.get("db", "akshare_realtime")]
        self.kline_collection = self.db[ak_cfg.get("kline", "kline")]
        self.daily_collection = self.db[ak_cfg.get("daily", "daily")]
        self.sleep_seconds = float(ak_cfg.get("sleep_seconds", 5))
        self.timeframes: Sequence[str] = tuple(ak_cfg.get("timeframes", ["5m"]))
        self.source_tag = ak_cfg.get("source_tag", "akshare")
        self.mirror_to_baostock = bool(ak_cfg.get("mirror_to_baostock", True))
        self.baostock_minute_col = None
        baostock_cfg = self.config.get("baostock", {})
        if self.mirror_to_baostock and baostock_cfg:
            try:
                baostock_db = self.client[baostock_cfg["db"]]
                self.baostock_minute_col = baostock_db[baostock_cfg["minute_5"]]
            except KeyError:
                self.mirror_to_baostock = False
        else:
            self.mirror_to_baostock = False

        self.symbols = self._load_symbols(ak_cfg.get("symbols"))
        if not self.symbols:
            raise ValueError("AkshareRealtimeManager: symbols 列表为空，请先在配置或 stock_basic 中提供股票列表。")

        self.aggregators: Dict[str, Dict[str, TimeframeAggregator]] = {
            timeframe: {
                symbol: TimeframeAggregator(
                    timeframe,
                    symbol,
                    self.kline_collection,
                    source_tag=self.source_tag,
                    temp_flag=True,
                    on_save=self._mirror_bar_to_baostock if self.mirror_to_baostock else None,
                )
                for symbol in self.symbols
            }
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

    def _to_baostock_code(self, symbol: Optional[str]) -> Optional[str]:
        if not symbol:
            return None
        token = symbol.strip()
        if len(token) < 6:
            return None
        if token.startswith("6"):
            return f"sh.{token}"
        if token.startswith(("0", "3")):
            return f"sz.{token}"
        if token.startswith(("4", "8")):
            return f"bj.{token}"
        return None

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
        """Use Sina quotes directly (avoid Eastmoney restrictions)."""
        return self._fetch_quotes_sina()

    def _fetch_quotes_direct(self) -> pd.DataFrame:
        """Fallback: call Eastmoney spot API with IPv4 + no proxy."""
        session = requests.Session()
        session.trust_env = False
        session.headers["User-Agent"] = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        url = "https://82.push2.eastmoney.com/api/qt/clist/get"
        base_params = {
            "po": "1",
            "np": "1",
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": "2",
            "invt": "2",
            "fid": "f12",
            # A股、科创、创业、北交所
            "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048",
            "fields": "f12,f2,f15,f16,f5",
        }
        all_items: List[Dict[str, Any]] = []
        page = 1
        page_size = 100
        total = None
        while True:
            params = {**base_params, "pn": str(page), "pz": str(page_size)}
            resp = session.get(
                url,
                params=params,
                timeout=10,
                proxies={"http": None, "https": None},
            )
            resp.raise_for_status()
            data = resp.json().get("data", {}) or {}
            items = data.get("diff") or []
            if total is None:
                total = data.get("total") or 0
            if not items:
                break
            all_items.extend(items)
            if total and len(all_items) >= total:
                break
            page += 1
            if page > 200:  # safety cap
                break

        if not all_items:
            return pd.DataFrame(columns=["代码", "最新价", "最高", "最低", "成交量"])
        df = pd.DataFrame(all_items)
        df = df.rename(
            columns={
                "f12": "代码",
                "f2": "最新价",
                "f15": "最高",
                "f16": "最低",
                "f5": "成交量",
            }
        )
        df["代码"] = df["代码"].astype(str)
        df["成交量"] = pd.to_numeric(df["成交量"], errors="coerce").fillna(0.0)
        return df

    def _fetch_quotes_sina(self) -> pd.DataFrame:
        """Fallback: Sina quote API (http://hq.sinajs.cn)."""
        if not self.symbols:
            return pd.DataFrame(columns=["代码", "最新价", "最高", "最低", "成交量"])

        def _to_sina_code(symbol: str) -> Optional[str]:
            token = symbol.upper()
            if token.startswith("SH"):
                return f"sh{token[2:]}"
            if token.startswith("SZ"):
                return f"sz{token[2:]}"
            if token.startswith("BJ"):
                return f"bj{token[2:]}"
            if token and token[0].isdigit():
                # 默认沪深
                if token.startswith("6"):
                    return f"sh{token}"
                return f"sz{token}"
            return None

        chunks: List[Dict[str, Any]] = []
        batch_size = 30  # 控制单次请求数量，避免 403
        sina_codes = [code for code in (_to_sina_code(s) for s in self.symbols) if code]
        session = requests.Session()
        session.trust_env = False
        session.headers["User-Agent"] = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        session.headers["Referer"] = "http://finance.sina.com.cn/"
        for i in range(0, len(sina_codes), batch_size):
            batch = sina_codes[i : i + batch_size]
            url = f"http://hq.sinajs.cn/list={','.join(batch)}"
            resp = session.get(url, timeout=10)
            resp.raise_for_status()
            for line in resp.text.splitlines():
                if not line:
                    continue
                try:
                    prefix, data = line.split("=", 1)
                    code = prefix.split("str_")[-1]
                    parts = data.strip('";\n').split(",")
                    if len(parts) < 6:
                        continue
                    name, open_px, prev_close, last, high, low, *rest = parts
                    volume = parts[8] if len(parts) > 8 else "0"
                    chunks.append(
                        {
                            "代码": code.upper().replace("SH", "").replace("SZ", "").replace("BJ", ""),
                            "最新价": float(last) if last else None,
                            "最高": float(high) if high else None,
                            "最低": float(low) if low else None,
                            "成交量": float(volume) if volume else 0.0,
                        }
                    )
                except Exception:
                    continue

        if not chunks:
            return pd.DataFrame(columns=["代码", "最新价", "最高", "最低", "成交量"])
        df = pd.DataFrame(chunks)
        df["代码"] = df["代码"].astype(str)
        df["成交量"] = pd.to_numeric(df["成交量"], errors="coerce").fillna(0.0)
        return df

    def _volume_increment(self, symbol: str, total_volume: float) -> float:
        if pd.isna(total_volume):
            return 0.0
        try:
            total_volume = float(total_volume)
        except (TypeError, ValueError):
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
                if symbol not in self.aggregators[timeframe]:
                    continue
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
                "source": self.source_tag,
                "temporary": True,
            }
            self.daily_collection.update_one(
                {"symbol": symbol, "date": trade_date},
                {"$set": doc},
                upsert=True,
            )
        except errors.PyMongoError as exc:
            logger.error("写入 Akshare 日线快照失败 %s: %s", symbol, exc)

    def _mirror_bar_to_baostock(self, bar: Dict[str, Any]) -> None:
        if self.baostock_minute_col is None:
            return
        code = self._to_baostock_code(bar.get("symbol"))
        bar_time = bar.get("datetime")
        if not code or not isinstance(bar_time, datetime):
            return
        trade_date = bar_time.strftime("%Y-%m-%d")
        trade_time = bar_time.strftime("%H:%M:%S")
        payload = {
            "code": code,
            "date": trade_date,
            "time": trade_time,
            "open": bar.get("open"),
            "high": bar.get("high"),
            "low": bar.get("low"),
            "close": bar.get("close"),
            "volume": bar.get("volume"),
            "amount": None,
            "adjustflag": None,
            "source": self.source_tag,
            "temporary": True,
        }
        self.baostock_minute_col.update_one(
            {"code": code, "date": trade_date, "time": trade_time},
            {"$set": payload},
            upsert=True,
        )

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
