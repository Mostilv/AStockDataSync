import json
import baostock as bs
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

import akshare as ak
from pymongo import ASCENDING, DESCENDING, MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.errors import BulkWriteError
from tqdm import tqdm

from ..utils.config_loader import load_config
from ..utils.backend_client import BackendClient

START_DATE = "2014-01-01"
MINUTE_START_DATE = "2019-01-02"
ADJUSTFLAG = "3"
DATE_FORMAT = "%Y-%m-%d"
RETRY_LIMIT = 3
DAYS_PER_YEAR = 365
SOURCE_BAOSTOCK = "baostock"
DEFAULT_INTEGRITY_WINDOWS = {
    "d": 30,
    "w": 400,
    "m": 1500,
    "5": 15,
}
DEFAULT_DAILY_CALL_LIMIT = 150_000
DEFAULT_CALL_TRACKER_PATH = Path.home() / ".astock_baostock_calls.json"
FINANCE_REPORTERS = {
    "profit": bs.query_profit_data,
    "balance": bs.query_balance_data,
    "cash_flow": bs.query_cash_flow_data,
    "dupont": bs.query_dupont_data,
}
FREQ_DESC = {
    "d": "同步日线",
    "w": "同步周线",
    "m": "同步月线",
    "5": "同步5分钟",
}


class DailyRateLimiter:
    """Persists Baostock API usage per calendar day to enforce a hard cap."""

    def __init__(self, limit: int = DEFAULT_DAILY_CALL_LIMIT, cache_path: Optional[str] = None) -> None:
        self.limit = max(0, int(limit or DEFAULT_DAILY_CALL_LIMIT))
        if cache_path:
            self.cache_path = Path(cache_path).expanduser().resolve()
        else:
            self.cache_path = DEFAULT_CALL_TRACKER_PATH
        self.current_date = datetime.now().strftime(DATE_FORMAT)
        self.count = 0
        self._load_state()

    def _load_state(self) -> None:
        try:
            data = json.loads(self.cache_path.read_text(encoding="utf-8"))
            if data.get("date") == self.current_date:
                self.count = int(data.get("count", 0))
        except FileNotFoundError:
            pass
        except (json.JSONDecodeError, OSError, ValueError):
            # Bad cache files are treated as zero usage
            self.count = 0

    def _persist(self) -> None:
        try:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            self.cache_path.write_text(
                json.dumps({"date": self.current_date, "count": self.count}),
                encoding="utf-8",
            )
        except OSError:
            # Unable to persist shouldn't break synchronization, but limit is still enforced in-memory
            pass

    def consume(self, cost: int = 1) -> None:
        if cost <= 0 or self.limit <= 0:
            return
        today = datetime.now().strftime(DATE_FORMAT)
        if today != self.current_date:
            self.current_date = today
            self.count = 0
        if self.count + cost > self.limit:
            raise RuntimeError(
                f"Baostock API 调用已达 {self.count} 次，超过每日上限 {self.limit}，"
                "请减少任务量或等待次日再继续。"
            )
        self.count += cost
        self._persist()


class BaostockManager:
    """Encapsulates Baostock data synchronization against MongoDB."""

    def __init__(self, config_path: str = "config.yaml", backend_client: Optional[BackendClient] = None):
        self.config = load_config(config_path)
        self.backend_client = backend_client

        mongo_cfg = self.config["mongodb"]
        baostock_cfg = self.config["baostock"]

        self.client = MongoClient(mongo_cfg["uri"])
        self.db = self.client[baostock_cfg["db"]]
        self.stock_basic_col = self.db[baostock_cfg["basic"]]
        self.daily_col = self.db[baostock_cfg["daily"]]
        self.weekly_col = self.db[baostock_cfg.get("weekly", "weekly_adjusted")]
        self.monthly_col = self.db[baostock_cfg.get("monthly", "monthly_adjusted")]
        self.minute_5_col = self.db[baostock_cfg["minute_5"]]
        self.finance_col = self.db[baostock_cfg.get("finance_quarterly", "finance_quarterly")]

        self.history_years = int(baostock_cfg.get("history_years", 10))
        self.finance_history_years = int(baostock_cfg.get("finance_history_years", 10))
        self.minute_start_date = baostock_cfg.get("minute_start_date", MINUTE_START_DATE)
        self.minute_lookback_days = (
            int(baostock_cfg.get("minute_lookback_days")) if baostock_cfg.get("minute_lookback_days") else None
        )
        self.default_frequencies: Tuple[str, ...] = tuple(
            baostock_cfg.get("frequencies", ["d", "w", "m", "5"])
        )
        self.index_codes: Tuple[str, ...] = tuple(
            code.strip()
            for code in baostock_cfg.get("index_codes", []) or []
            if str(code).strip()
        )
        self.tagging_cfg: Dict[str, Any] = baostock_cfg.get("tagging", {}) or {}
        integrity_cfg = baostock_cfg.get("integrity_windows", {})
        self.integrity_windows: Dict[str, int] = {
            freq: int(integrity_cfg.get(freq, DEFAULT_INTEGRITY_WINDOWS.get(freq, 0)) or 0)
            for freq in set(DEFAULT_INTEGRITY_WINDOWS) | set(integrity_cfg)
        }
        daily_limit = int(baostock_cfg.get("daily_call_limit", DEFAULT_DAILY_CALL_LIMIT))
        tracker_path = baostock_cfg.get("call_tracker_path")
        self.rate_limiter = DailyRateLimiter(limit=daily_limit, cache_path=tracker_path)

        self.collection_meta: Dict[str, Dict[str, Any]] = {
            "d": {
                "collection": self.daily_col,
                "field": "last_daily_date",
                "min_start": START_DATE,
                "default_years": self.history_years,
            },
            "w": {
                "collection": self.weekly_col,
                "field": "last_weekly_date",
                "min_start": START_DATE,
                "default_years": self.history_years,
            },
            "m": {
                "collection": self.monthly_col,
                "field": "last_monthly_date",
                "min_start": START_DATE,
                "default_years": self.history_years,
            },
            "5": {
                "collection": self.minute_5_col,
                "field": "last_minute_5_date",
                "min_start": self.minute_start_date,
                "default_years": None,
                "lookback_days": self.minute_lookback_days,
            },
        }

        self._create_indexes()

        lg = bs.login()
        if lg.error_code != "0":
            raise RuntimeError(f"登录baostock失败: {lg.error_msg}")
        print("登录baostock成功")

    # ------------------------------------------------------------------ #
    # Lifecycle helpers
    # ------------------------------------------------------------------ #
    def _create_indexes(self) -> None:
        if "code_1" not in self.stock_basic_col.index_information():
            self.stock_basic_col.create_index([("code", ASCENDING)], unique=True)

        for col in (self.daily_col, self.weekly_col, self.monthly_col):
            if "code_1_date_1" not in col.index_information():
                col.create_index([("code", ASCENDING), ("date", ASCENDING)], unique=True)

        if "code_1_date_1_time_1" not in self.minute_5_col.index_information():
            self.minute_5_col.create_index(
                [("code", ASCENDING), ("date", ASCENDING), ("time", ASCENDING)],
                unique=True,
            )

        if "code_1_year_1_quarter_1_report_type_1" not in self.finance_col.index_information():
            self.finance_col.create_index(
                [
                    ("code", ASCENDING),
                    ("year", ASCENDING),
                    ("quarter", ASCENDING),
                    ("report_type", ASCENDING),
                ],
                unique=True,
            )

    def close(self) -> None:
        bs.logout()
        self.client.close()
        print("已登出baostock并关闭MongoDB连接")

    def __enter__(self) -> "BaostockManager":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    # ------------------------------------------------------------------ #
    # 基础信息
    # ------------------------------------------------------------------ #
    def query_stock_basic(self, refresh: bool = False) -> None:
        expected_fields = ["code", "code_name", "ipoDate", "outDate", "type", "status"]
        self.rate_limiter.consume()
        rs = bs.query_stock_basic()
        if rs.fields != expected_fields:
            raise ValueError(
                f"query_stock_basic字段不匹配，预期 {expected_fields}，实际 {rs.fields}"
            )

        stock_list: List[Dict[str, Any]] = []
        while rs.next():
            row = rs.get_row_data()
            if row[5] == "1" and row[4] in ("1", "2"):
                stock_list.append(
                    {
                        "code": row[0],
                        "code_name": row[1],
                        "ipoDate": row[2],
                        "outDate": row[3],
                        "type": row[4],
                        "status": row[5],
                        "source": SOURCE_BAOSTOCK,
                        "temporary": False,
                    }
                )

        if refresh:
            self.stock_basic_col.delete_many({})
            if stock_list:
                self.stock_basic_col.insert_many(stock_list)
        else:
            for stock in stock_list:
                self.stock_basic_col.update_one(
                    {"code": stock["code"]},
                    {"$set": stock},
                    upsert=True,
                )
            print(f"股票基本信息更新完成，共处理 {len(stock_list)} 条记录。")
            if self.backend_client:
                self.backend_client.push_stock_basic(stock_list)

    def refresh_industry_and_concepts(
        self,
        include_industry: bool = True,
    ) -> None:
        """Persist Shenwan L1 industry tags into stock_basic."""
        if not include_industry:
            return

        industry_map: Dict[str, Dict[str, str]] = self._load_sw_industry_mapping()
        if industry_map:
            print(f"???{len(industry_map)} ??????????")
        else:
            print("?????????????")
            return

        for code, payload in industry_map.items():
            if not payload:
                continue
            try:
                self.stock_basic_col.update_one(
                    {"code": code},
                    {"$set": payload, "$setOnInsert": {"source": SOURCE_BAOSTOCK, "temporary": True}},
                    upsert=True,
                )
            except Exception as exc:  # noqa: BLE001
                tqdm.write(f"???????? {code}: {exc}")


    # ------------------------------------------------------------------ #
    # K 线同步
    # ------------------------------------------------------------------ #
    def query_history_k_data_plus(
        self,
        code: str,
        start_date: str,
        end_date: str,
        frequency: str = "d",
    ) -> List[Dict[str, Any]]:
        if frequency == "d":
            fields = (
                "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,"
                "tradestatus,pctChg,peTTM,psTTM,pcfNcfTTM,pbMRQ,isST"
            )
            expected_fields = [
                "date",
                "code",
                "open",
                "high",
                "low",
                "close",
                "preclose",
                "volume",
                "amount",
                "adjustflag",
                "turn",
                "tradestatus",
                "pctChg",
                "peTTM",
                "psTTM",
                "pcfNcfTTM",
                "pbMRQ",
                "isST",
            ]
        elif frequency == "5":
            fields = "date,time,code,open,high,low,close,volume,amount,adjustflag"
            expected_fields = [
                "date",
                "time",
                "code",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "amount",
                "adjustflag",
            ]
        elif frequency in ("w", "m"):
            fields = "date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg"
            expected_fields = [
                "date",
                "code",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "amount",
                "adjustflag",
                "turn",
                "pctChg",
            ]
        else:
            raise ValueError("Unsupported frequency. Choose from d/w/m/5.")

        for attempt in range(RETRY_LIMIT):
            try:
                self.rate_limiter.consume()
                rs = bs.query_history_k_data_plus(
                    code,
                    fields,
                    start_date,
                    end_date,
                    frequency,
                    adjustflag=ADJUSTFLAG,
                )
                if rs.error_code != "0":
                    print(
                        f"获取{code}{frequency}K线失败: {rs.error_msg} "
                        f"(尝试 {attempt + 1}/{RETRY_LIMIT})"
                    )
                    time.sleep(1)
                    continue

                if rs.fields != expected_fields:
                    raise ValueError(
                        f"{code} {frequency} 字段不匹配，预期 {expected_fields}，实际 {rs.fields}"
                    )

                data_list: List[Dict[str, Any]] = []
                while rs.next():
                    row = rs.get_row_data()
                    data_list.append(self._normalize_row(rs.fields, row))

                data_list.sort(key=lambda x: x["date"])
                return data_list
            except Exception as exc:  # noqa: BLE001
                print(
                    f"获取{code} {frequency}级别K线异常: {exc} "
                    f"(尝试 {attempt + 1}/{RETRY_LIMIT})"
                )
                time.sleep(1)

        print(f"获取{code} {frequency}级别K线失败，超出最大重试次数。")
        return []

    def query_all_stock(self, day: str = "2024-10-25") -> None:
        self.rate_limiter.consume()
        rs = bs.query_all_stock(day)
        print(f"query_all_stock 暂未落库，返回字段: {rs.fields}")

    def sync_k_data(
        self,
        frequencies: Optional[Sequence[str]] = None,
        full_update: bool = False,
        lookback_years: Optional[int] = None,
        dry_run: bool = False,
        resume: bool = False,
    ) -> None:
        freq_list = tuple(frequencies or self.default_frequencies)
        projection = {"code": 1}
        for meta in self.collection_meta.values():
            projection[meta["field"]] = 1

        stock_list = self._compose_target_stock_list(projection)
        if not stock_list:
            print("未找到股票基础信息，跳过K线同步。")
            return

        end_dt = datetime.now()
        end_date_str = end_dt.strftime(DATE_FORMAT)

        for freq in freq_list:
            settings = self._get_collection_meta(freq)
            collection: Collection = settings["collection"]
            field_name: str = settings["field"]
            min_start = settings["min_start"]
            default_years = settings["default_years"]
            lookback_days = settings.get("lookback_days")
            freq_lookback_years = lookback_years if (lookback_years is not None and freq != "5") else default_years

            desc = FREQ_DESC.get(freq, f"同步{freq}数据")
            with tqdm(
                total=len(stock_list),
                desc=desc,
                unit="stock",
                dynamic_ncols=True,
            ) as pbar:
                for stock in stock_list:
                    code = stock["code"]
                    last_date = stock.get(field_name) or self._latest_date_in_collection(collection, code)
                    start_date_str = self._resolve_start_date(
                        last_date,
                        full_update,
                        freq_lookback_years,
                        end_dt,
                        min_start,
                        resume_from_existing=resume,
                        lookback_days=lookback_days,
                    )
                    if not start_date_str or start_date_str > end_date_str:
                        pbar.update(1)
                        continue

                    if dry_run:
                        tqdm.write(f"[Dry Run] {code} -> {freq} {start_date_str} ~ {end_date_str}")
                        pbar.update(1)
                        continue

                    data_list = self.query_history_k_data_plus(
                        code,
                        start_date_str,
                        end_date_str,
                        freq,
                    )
                    if not data_list:
                        pbar.update(1)
                        continue

                    new_last_date = self._bulk_upsert_kline(collection, freq, data_list)
                    if new_last_date:
                        self.stock_basic_col.update_one(
                            {"code": code},
                            {"$set": {field_name: new_last_date}},
                            upsert=True,
                        )
                        tqdm.write(
                            f"{code} {freq} 数据更新 {len(data_list)} 条，最新日期 {new_last_date}"
                        )
                        if self.backend_client:
                             self.backend_client.push_kline(data_list, freq)
                    pbar.update(1)

    def sync_limit_up_minute_data(
        self,
        days: int = 7,
        frequencies: Sequence[str] = ("5",),
        pct_threshold: float = 9.5,
    ) -> None:
        """Sync minute data for limit-up stocks within the recent window."""
        if not frequencies:
            print("未提供分钟频率，跳过涨停分钟同步。")
            return

        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=max(1, days))
        end_date_str = end_dt.strftime(DATE_FORMAT)

        freq_list = [freq for freq in frequencies if freq == "5"]
        if not freq_list:
            print("分钟频率仅支持 5，已跳过。")
            return

        date_filter = {
            "$gte": start_dt.strftime(DATE_FORMAT),
            "$lte": end_date_str,
        }
        query = {"date": date_filter, "pctChg": {"$gte": pct_threshold}}
        codes = sorted(
            {doc["code"] for doc in self.daily_col.find(query, {"code": 1}) if doc.get("code")}
        )
        if not codes:
            print(f"近 {days} 天未找到涨停股票，跳过分钟数据同步。")
            return

        print(f"近 {days} 天涨停股票数量：{len(codes)}，开始同步分钟数据 {freq_list}。")
        for freq in freq_list:
            settings = self._get_collection_meta(freq)
            collection: Collection = settings["collection"]
            min_start = datetime.strptime(settings["min_start"], DATE_FORMAT)
            start_date_str = max(start_dt, min_start).strftime(DATE_FORMAT)

            with tqdm(
                total=len(codes),
                desc=f"涨停{freq}分钟同步",
                unit="stock",
                dynamic_ncols=True,
            ) as pbar:
                for code in codes:
                    data_list = self.query_history_k_data_plus(
                        code,
                        start_date_str,
                        end_date_str,
                        freq,
                    )
                    if not data_list:
                        pbar.update(1)
                        continue

                    new_last_date = self._bulk_upsert_kline(collection, freq, data_list)
                    if new_last_date:
                        self.stock_basic_col.update_one(
                            {"code": code},
                            {"$set": {settings["field"]: new_last_date}},
                            upsert=True,
                        )
                        if self.backend_client:
                            self.backend_client.push_kline(data_list, freq)
                    pbar.update(1)

    def update_stock_k_data(self, full_update: bool = False) -> None:
        self.sync_k_data(full_update=full_update)

    # ------------------------------------------------------------------ #
    # 财务数据
    # ------------------------------------------------------------------ #
    def sync_finance_data(
        self,
        full_update: bool = False,
        years: Optional[int] = None,
        dry_run: bool = False,
        resume: bool = False,
    ) -> None:
        projection = {"code": 1, "last_finance_quarter": 1}
        stock_list = list(self.stock_basic_col.find({}, projection))
        if not stock_list:
            print("未找到股票基础信息，跳过财务数据同步。")
            return

        end_year, end_quarter = self._current_year_quarter()
        lookback = years or self.finance_history_years
        min_year = end_year - lookback + 1

        with tqdm(
            total=len(stock_list),
            desc="同步季频财务数据",
            unit="stock",
            dynamic_ncols=True,
        ) as pbar:
            for stock in stock_list:
                code = stock["code"]
                marker = stock.get("last_finance_quarter")
                if not marker:
                    marker = self._latest_finance_marker(code)

                use_marker = bool(marker) and (resume or not full_update)
                if use_marker and marker:
                    start_year, start_quarter = self._next_quarter(*self._parse_quarter_marker(marker))
                else:
                    start_year, start_quarter = min_year, 1

                if start_year < min_year:
                    start_year = min_year
                    start_quarter = 1

                quarters = list(self._iter_finance_quarters(start_year, start_quarter, end_year, end_quarter))
                if not quarters:
                    pbar.update(1)
                    continue

                if dry_run:
                    tqdm.write(
                        f"[Dry Run] {code} -> finance {quarters[0][0]}Q{quarters[0][1]} "
                        f"~ {quarters[-1][0]}Q{quarters[-1][1]}"
                    )
                    pbar.update(1)
                    continue

                latest_marker = None
                for year, quarter in quarters:
                    data_written = False
                    for report_type, reporter in FINANCE_REPORTERS.items():
                        rows = self._query_finance_dataset(reporter, code, year, quarter)
                        if not rows:
                            continue
                        try:
                            operations = []
                            for row in rows:
                                payload = {
                                    **row,
                                    "code": row.get("code", code),
                                    "year": int(row.get("year", year)),
                                    "quarter": int(row.get("quarter", quarter)),
                                    "report_type": report_type,
                                }
                                operations.append(
                                    UpdateOne(
                                        {
                                            "code": payload["code"],
                                            "year": payload["year"],
                                            "quarter": payload["quarter"],
                                            "report_type": payload["report_type"],
                                        },
                                        {"$set": payload},
                                        upsert=True,
                                    )
                                )
                            if operations:
                                self.finance_col.bulk_write(operations, ordered=False)
                                data_written = True
                        except BulkWriteError as exc:  # noqa: BLE001
                            tqdm.write(f"{code} 财务数据写入失败: {exc.details}")

                    if data_written:
                        latest_marker = self._format_quarter_marker(year, quarter)

                if latest_marker:
                    self.stock_basic_col.update_one(
                        {"code": code},
                        {"$set": {"last_finance_quarter": latest_marker}},
                    )
                    tqdm.write(f"{code} 财务数据更新至 {latest_marker}")
                pbar.update(1)

    # ------------------------------------------------------------------ #
    # 数据完整性巡检
    # ------------------------------------------------------------------ #
    def run_integrity_check(
        self,
        frequencies: Optional[Sequence[str]] = None,
        window_days: Optional[Dict[str, int]] = None,
        dry_run: bool = False,
    ) -> None:
        freq_list = tuple(frequencies or self.default_frequencies)
        projection = {"code": 1}
        for meta in self.collection_meta.values():
            projection[meta["field"]] = 1
        stock_list = list(self.stock_basic_col.find({}, projection))
        if not stock_list:
            print("未找到股票基础信息，跳过完整性校验。")
            return

        freq_windows = window_days or self.integrity_windows
        end_dt = datetime.now()
        end_date_str = end_dt.strftime(DATE_FORMAT)

        for freq in freq_list:
            window = int(freq_windows.get(freq, 0) or 0)
            if window <= 0:
                continue

            settings = self._get_collection_meta(freq)
            collection: Collection = settings["collection"]
            min_start = datetime.strptime(settings["min_start"], DATE_FORMAT)
            start_dt = max(min_start, end_dt - timedelta(days=window))
            start_date_str = start_dt.strftime(DATE_FORMAT)

            desc = f"校验{freq}数据"
            with tqdm(
                total=len(stock_list),
                desc=desc,
                unit="stock",
                dynamic_ncols=True,
            ) as pbar:
                for stock in stock_list:
                    code = stock["code"]
                    if dry_run:
                        tqdm.write(
                            f"[Integrity Dry Run] {code} -> {freq} {start_date_str} ~ {end_date_str}"
                        )
                        pbar.update(1)
                        continue

                    data_list = self.query_history_k_data_plus(
                        code,
                        start_date_str,
                        end_date_str,
                        freq,
                    )
                    if not data_list:
                        pbar.update(1)
                        continue

                    new_last_date = self._bulk_upsert_kline(collection, freq, data_list)
                    if new_last_date:
                        field_name = settings["field"]
                        self.stock_basic_col.update_one(
                            {"code": code},
                            {"$set": {field_name: new_last_date}},
                            upsert=True,
                        )
                    pbar.update(1)

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _get_collection_meta(self, frequency: str) -> Dict[str, Any]:
        if frequency not in self.collection_meta:
            raise ValueError(f"不支持的频率 {frequency}，允许 {list(self.collection_meta.keys())}")
        return self.collection_meta[frequency]

    def _compose_target_stock_list(self, projection: Dict[str, int]) -> List[Dict[str, Any]]:
        """Combine stock_basic with configured index codes for synchronization."""
        stock_list = list(self.stock_basic_col.find({}, projection))
        existing_codes = {item.get("code") for item in stock_list}
        for code in self.index_codes:
            if code in existing_codes:
                continue
            placeholder = {
                "code": code,
                "source": SOURCE_BAOSTOCK,
                "temporary": True,
            }
            self.stock_basic_col.update_one({"code": code}, {"$setOnInsert": placeholder}, upsert=True)
            stock_list.append({"code": code})
        return stock_list

    def _latest_date_in_collection(self, collection: Collection, code: str) -> Optional[str]:
        record = collection.find_one({"code": code}, {"date": 1}, sort=[("date", DESCENDING)])
        return record["date"] if record else None

    def needs_backfill(self, frequency: str, expected_date: Optional[str] = None) -> bool:
        settings = self._get_collection_meta(frequency)
        collection: Collection = settings["collection"]
        field_name: str = settings["field"]

        if collection.count_documents({}) == 0:
            return True
        missing = self.stock_basic_col.count_documents({field_name: {"$exists": False}})
        if missing > 0:
            return True
        if expected_date:
            stale = self.stock_basic_col.count_documents({field_name: {"$lt": expected_date}})
            if stale > 0:
                return True
        return False

    def _resolve_start_date(
        self,
        last_date: Optional[str],
        full_update: bool,
        lookback_years: Optional[int],
        end_dt: datetime,
        min_start_date: str,
        resume_from_existing: bool = False,
        lookback_days: Optional[int] = None,
    ) -> Optional[str]:
        min_start_dt = datetime.strptime(min_start_date, DATE_FORMAT)
        if last_date and (resume_from_existing or not full_update):
            candidate = datetime.strptime(last_date, DATE_FORMAT) + timedelta(days=1)
        elif full_update:
            candidate = min_start_dt
        elif lookback_years:
            lookback_dt = end_dt - timedelta(days=lookback_years * DAYS_PER_YEAR)
            candidate = max(min_start_dt, lookback_dt)
        elif lookback_days:
            lookback_dt = end_dt - timedelta(days=lookback_days)
            candidate = max(min_start_dt, lookback_dt)
        else:
            candidate = min_start_dt

        if candidate > end_dt:
            return None
        return candidate.strftime(DATE_FORMAT)

    def _normalize_row(self, fields: Sequence[str], row: Sequence[str]) -> Dict[str, Any]:
        data: Dict[str, Any] = {}
        for key, value in zip(fields, row):
            if value in ("", None):
                data[key] = None
                continue
            try:
                data[key] = float(value)
            except ValueError:
                data[key] = value
        data["source"] = SOURCE_BAOSTOCK
        data["temporary"] = False
        return data

    def _bulk_upsert_kline(
        self,
        collection: Collection,
        frequency: str,
        data_list: List[Dict[str, Any]],
    ) -> Optional[str]:
        try:
            operations: List[UpdateOne] = []
            for data in data_list:
                filter_doc = {
                    "code": data["code"],
                    "date": data["date"],
                }
                if "time" in data and data["time"]:
                    filter_doc["time"] = data["time"]
                operations.append(
                    UpdateOne(
                        filter_doc,
                        {"$set": data},
                        upsert=True,
                    )
                )
            if operations:
                collection.bulk_write(operations, ordered=False)
                return data_list[-1]["date"]
        except BulkWriteError as exc:  # noqa: BLE001
            tqdm.write(f"{data_list[0].get('code')} {frequency} 批量写入失败: {exc.details}")
        return None

    def _current_year_quarter(self) -> Tuple[int, int]:
        now = datetime.now()
        quarter = (now.month - 1) // 3 + 1
        return now.year, quarter

    def _format_quarter_marker(self, year: int, quarter: int) -> str:
        return f"{year}Q{quarter}"

    def _parse_quarter_marker(self, marker: str) -> Tuple[int, int]:
        year_str, quarter_str = marker.split("Q")
        return int(year_str), int(quarter_str)

    def _next_quarter(self, year: int, quarter: int) -> Tuple[int, int]:
        if quarter == 4:
            return year + 1, 1
        return year, quarter + 1

    def _iter_finance_quarters(
        self,
        start_year: int,
        start_quarter: int,
        end_year: int,
        end_quarter: int,
    ) -> Iterator[Tuple[int, int]]:
        year, quarter = start_year, start_quarter
        while (year < end_year) or (year == end_year and quarter <= end_quarter):
            yield year, quarter
            year, quarter = self._next_quarter(year, quarter)

    def _latest_finance_marker(self, code: str) -> Optional[str]:
        record = self.finance_col.find_one(
            {"code": code},
            {"year": 1, "quarter": 1},
            sort=[("year", DESCENDING), ("quarter", DESCENDING)],
        )
        if record:
            return self._format_quarter_marker(int(record["year"]), int(record["quarter"]))
        return None

    def _query_finance_dataset(
        self,
        reporter,
        code: str,
        year: int,
        quarter: int,
    ) -> List[Dict[str, Any]]:
        for attempt in range(RETRY_LIMIT):
            try:
                self.rate_limiter.consume()
                rs = reporter(code=code, year=year, quarter=quarter)
                if rs.error_code != "0":
                    print(
                        f"获取{code} {year}Q{quarter} 财务数据失败: {rs.error_msg} "
                        f"(尝试 {attempt + 1}/{RETRY_LIMIT})"
                    )
                    time.sleep(1)
                    continue

                rows: List[Dict[str, Any]] = []
                while rs.next():
                    row = rs.get_row_data()
                    rows.append(self._normalize_row(rs.fields, row))
                return rows
            except Exception as exc:  # noqa: BLE001
                print(
                    f"获取{code} {year}Q{quarter} 财务数据异常: {exc} "
                    f"(尝试 {attempt + 1}/{RETRY_LIMIT})"
                )
                time.sleep(1)

        print(f"获取{code} {year}Q{quarter} 财务数据失败，超出最大重试次数。")
        return []

    # ------------------------------------------------------------------ #
    # Tag helpers (industry)
    # ------------------------------------------------------------------ #
    @staticmethod
    def _normalize_sw_component(code: str) -> Optional[str]:
        token = (code or "").strip().upper().replace(".SI", "").replace(".", "")
        if not token:
            return None
        if token.startswith(("60", "68", "56", "66")):
            return f"sh.{token[:6]}"
        if token.startswith(("00", "30")):
            return f"sz.{token[:6]}"
        if token.startswith(("43", "83", "87")):
            return f"bj.{token[:6]}"
        if len(token) >= 6:
            return f"sz.{token[:6]}"
        return None

    def _load_sw_industry_mapping(self) -> Dict[str, Dict[str, str]]:
        try:
            df = ak.sw_index_first_info()
        except Exception as exc:  # noqa: BLE001
            tqdm.write(f"加载申万一级行业列表失败: {exc}")
            return {}
        if df is None or df.empty:
            return {}
        df = df[["行业代码", "行业名称"]].dropna()
        industry_map: Dict[str, Dict[str, str]] = {}
        for _, row in df.iterrows():
            code = str(row["行业代码"]).replace(".SI", "").strip()
            name = str(row["行业名称"]).strip()
            try:
                members = ak.index_component_sw(code)
            except Exception as exc:  # noqa: BLE001
                tqdm.write(f"获取行业成分失败 {code}: {exc}")
                continue
            if members is None or members.empty:
                continue
            for _, r in members.iterrows():
                stock_code = self._normalize_sw_component(str(r.get("证券代码") or ""))
                if not stock_code:
                    continue
                industry_map[stock_code] = {
                    "industry_sw_code": code,
                    "industry_sw_name": name,
                }
        return industry_map
