import logging
import random
import time
from datetime import datetime, timedelta
import socket
import os
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
import requests

import urllib.request
urllib.request.getproxies = lambda: {}  # Force urllib to ignore proxy environment completely

os.environ["http_proxy"] = ""
os.environ["https_proxy"] = ""
os.environ["HTTP_PROXY"] = ""
os.environ["HTTPS_PROXY"] = ""
os.environ["all_proxy"] = ""
os.environ["ALL_PROXY"] = ""

import requests
# Monkeypatch requests to always ignore proxies
_orig_merge = requests.Session.merge_environment_settings
def _patched_merge(self, url, proxies, stream, verify, cert):
    settings = _orig_merge(self, url, proxies, stream, verify, cert)
    settings['proxies'] = {}  # force empty
    return settings
requests.Session.merge_environment_settings = _patched_merge

import akshare as ak
import pandas as pd
import pytz
from pymongo import ASCENDING, DESCENDING, MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.errors import BulkWriteError
from tqdm import tqdm

from requests.packages.urllib3.util import connection

from ..utils.config_loader import load_config
from ..utils.backend_client import BackendClient

BEIJING_TZ = pytz.timezone("Asia/Shanghai")
TRADING_DAYS = {0, 1, 2, 3, 4}
TRADING_PERIODS = [("09:30", "11:30"), ("13:00", "15:00")]
DATE_FORMAT = "%Y-%m-%d"
START_DATE = "2020-01-01"  # Default fallback

logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Force IPv4 to avoid Some ISP blocking Eastmoney IPv6 IPs
connection.allowed_gai_family = lambda: socket.AF_INET


class AkshareManager:
    """
    Comprehensive Data Manager using exclusively Akshare.
    - Historical Initial Backfill (3 years)
    - Intraday 30-min Snapshots (spot_em)
    - EOD Validation
    """

    def __init__(self, config_path: str = "config.yaml", backend_client: Optional[BackendClient] = None) -> None:
        self.config = load_config(config_path)
        self.backend_client = backend_client

        ak_cfg = self.config.get("akshare", {})
        mongo_cfg = self.config.get("mongodb", {})
        mongo_uri = ak_cfg.get("uri", mongo_cfg.get("uri", "mongodb://localhost:27017/"))

        self.client = MongoClient(mongo_uri)
        self.db = self.client[ak_cfg.get("db", "akshare_data")]
        
        self.stock_basic_col = self.db[ak_cfg.get("basic", "stock_basic")]
        self.daily_col = self.db[ak_cfg.get("daily", "daily_adjusted")]
        self.weekly_col = self.db[ak_cfg.get("weekly", "weekly_adjusted")]
        self.monthly_col = self.db[ak_cfg.get("monthly", "monthly_adjusted")]
        self.minute_5_col = self.db[ak_cfg.get("minute_5", "minute_5_adjusted")]

        self.history_years = int(ak_cfg.get("history_years", 3))
        self.minute_lookback_days = int(ak_cfg.get("minute_lookback_days", 30))
        self.default_frequencies = tuple(ak_cfg.get("frequencies", ["d", "w", "m", "5"]))
        
        self.source_tag = "akshare"
        self.sleep_seconds = float(ak_cfg.get("sleep_seconds", 5))

        self.collection_meta: Dict[str, Dict[str, Any]] = {
            "d": {
                "collection": self.daily_col,
                "field": "last_daily_date",
                "ak_period": "daily",
            },
            "w": {
                "collection": self.weekly_col,
                "field": "last_weekly_date",
                "ak_period": "weekly",
            },
            "m": {
                "collection": self.monthly_col,
                "field": "last_monthly_date",
                "ak_period": "monthly",
            },
            "5": {
                "collection": self.minute_5_col,
                "field": "last_minute_5_date",
                "ak_period": "5",
            },
        }

        self._ensure_indexes()
        
        # In-memory cache for synthesizing 30-minute K-lines from spot data
        self.spot_bar_cache = {}

    def _ensure_indexes(self) -> None:
        self.stock_basic_col.create_index([("code", ASCENDING)], unique=True)
        for col in (self.daily_col, self.weekly_col, self.monthly_col):
            col.create_index([("code", ASCENDING), ("date", ASCENDING)], unique=True)
        self.minute_5_col.create_index([("code", ASCENDING), ("date", ASCENDING), ("time", ASCENDING)], unique=True)

    def close(self) -> None:
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    # ------------------------------------------------------------------ #
    # Basic Info
    # ------------------------------------------------------------------ #
    def query_stock_basic(self) -> None:
        """Fetch all A-share stocks from stock_zh_a_spot_em to initialize basic info."""
        print("Fetching stock basic info via ak.stock_zh_a_spot_em...")
        try:
            df = ak.stock_zh_a_spot_em()
            if df.empty:
                logger.error("Empty dataframe from stock_zh_a_spot_em")
                return
            
            # Map columns
            # df columns: ['序号', '代码', '名称', '最新价', '涨跌幅', '涨跌额', '成交量', '成交额', '振幅', '最高', '最低', '今开', '昨收', '量比', '换手率', '市盈率-动态', '市净率', '总市值', '流通市值', '涨速', '5分钟涨跌', '60日涨跌幅', '年初至今涨跌幅']
            stock_list = []
            for _, row in df.iterrows():
                code = str(row["代码"])
                name = str(row["名称"])
                # Simple exclusion of B shares or non A-shares if necessary
                if not code.startswith(("6", "0", "3", "4", "8")):
                    continue
                    
                stock_list.append({
                    "code": code,
                    "code_name": name,
                    "source": self.source_tag,
                    "temporary": False
                })
            
            if stock_list:
                for stock in stock_list:
                    self.stock_basic_col.update_one(
                        {"code": stock["code"]},
                        {"$set": stock},
                        upsert=True
                    )
                print(f"Stock basic updated. Total stocks: {len(stock_list)}")
                
                # Push to backend
                if self.backend_client:
                    self.backend_client.push_stock_basic(stock_list)
                    
        except Exception as e:
            logger.error(f"Failed to fetch stock basic: {e}")

    # ------------------------------------------------------------------ #
    # Historical K-Line Backfill (Rate Limited)
    # ------------------------------------------------------------------ #
    def _safe_fetch_hist(self, code: str, start_date: str, end_date: str, freq: str) -> pd.DataFrame:
        """Wrap Akshare calls with backoff to avoid ban."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Sleep between requests during massive backfill
                time.sleep(random.uniform(0.5, 2.0))
                if freq in ["daily", "weekly", "monthly"]:
                    df = ak.stock_zh_a_hist(symbol=code, period=freq, start_date=start_date.replace("-", ""), end_date=end_date.replace("-", ""), adjust="qfq")
                elif freq == "5":
                    # minute data format requires datetime string e.g. "2024-03-01 09:30:00"
                    df = ak.stock_zh_a_hist_min_em(symbol=code, start_date=start_date + " 09:00:00", end_date=end_date + " 15:30:00", period="5", adjust="qfq")
                else:
                    return pd.DataFrame()
                return df
            except Exception as e:
                logger.warning(f"Error fetching hist for {code} {freq} (Attempt {attempt+1}): {e}")
                time.sleep(random.uniform(5.0, 10.0))
        return pd.DataFrame()

    def needs_backfill(self, frequency: str, expected_date: Optional[str] = None) -> bool:
        meta = self._get_collection_meta(frequency)
        collection = meta["collection"]
        field = meta["field"]
        if collection.count_documents({}) == 0:
            return True
        missing = self.stock_basic_col.count_documents({field: {"$exists": False}})
        return missing > 0

    def sync_k_data(self, frequencies: Sequence[str] = None, full_update: bool = False, lookback_years: int = 3, resume: bool = True) -> None:
        """Historical Initial/Gap Backfill"""
        freq_list = frequencies or self.default_frequencies
        stock_list = list(self.stock_basic_col.find({}, {"code": 1, "last_daily_date": 1, "last_weekly_date": 1, "last_monthly_date": 1, "last_minute_5_date": 1}))
        if not stock_list:
            print("No stock basic info found. Call query_stock_basic first.")
            return

        end_dt = datetime.now()
        end_date_str = end_dt.strftime(DATE_FORMAT)

        for freq in freq_list:
            meta = self._get_collection_meta(freq)
            collection = meta["collection"]
            field_name = meta["field"]
            ak_period = meta["ak_period"]
            
            # Start date calculation based on config
            if freq == "5":
                start_dt = end_dt - timedelta(days=self.minute_lookback_days)
            else:
                start_dt = end_dt - timedelta(days=lookback_years * 365)
                
            base_start_str = start_dt.strftime(DATE_FORMAT)
            
            with tqdm(total=len(stock_list), desc=f"Backfilling {freq} Data", dynamic_ncols=True) as pbar:
                for stock in stock_list:
                    code = stock["code"]
                    last_date = stock.get(field_name)
                    
                    if not full_update and resume and last_date:
                        query_start = (datetime.strptime(last_date, DATE_FORMAT) + timedelta(days=1)).strftime(DATE_FORMAT)
                    else:
                        query_start = base_start_str
                        
                    if query_start > end_date_str:
                        pbar.update(1)
                        continue
                        
                    df = self._safe_fetch_hist(code, query_start, end_date_str, ak_period)
                    if df is not None and not df.empty:
                        docs, new_last_date = self._transform_and_save_hist(df, freq, code, collection)
                        if docs:
                            self.stock_basic_col.update_one({"code": code}, {"$set": {field_name: new_last_date}})
                            # Push chunks
                            if self.backend_client:
                                self.backend_client.push_kline(docs, freq)
                    pbar.update(1)

    def _transform_and_save_hist(self, df: pd.DataFrame, freq: str, code: str, collection: Collection) -> Tuple[List[Dict[str, Any]], str]:
        """Convert dataframe to dicts, bulk insert."""
        docs = []
        last_date = ""
        for _, row in df.iterrows():
            if freq == "5":
                # '时间' col is "2023-10-18 10:30:00"
                dt_str = str(row["时间"])
                try:
                    dt_obj = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    continue
                d_date = dt_obj.strftime("%Y-%m-%d")
                d_time = dt_obj.strftime("%H:%M:%S")
                last_date = max(last_date, d_date) if last_date else d_date
                doc = {
                    "code": code,
                    "date": d_date,
                    "time": d_time,
                    "open": float(row.get("开盘", 0)),
                    "close": float(row.get("收盘", 0)),
                    "high": float(row.get("最高", 0)),
                    "low": float(row.get("最低", 0)),
                    "volume": float(row.get("成交量", 0)),
                    "amount": float(row.get("成交额", 0)),
                    "adjustflag": "3",  # qfq
                    "source": self.source_tag,
                    "temporary": False
                }
            else:
                # '日期' col is "2023-10-18" or timestamp
                d_date = str(row["日期"])[:10]
                last_date = max(last_date, d_date) if last_date else d_date
                doc = {
                    "code": code,
                    "date": d_date,
                    "open": float(row.get("开盘", 0)),
                    "close": float(row.get("收盘", 0)),
                    "high": float(row.get("最高", 0)),
                    "low": float(row.get("最低", 0)),
                    "volume": float(row.get("成交量", 0)),
                    "amount": float(row.get("成交额", 0)),
                    "turn": float(row.get("换手率", 0)) if "换手率" in row else None,
                    "pctChg": float(row.get("涨跌幅", 0)) if "涨跌幅" in row else None,
                    "adjustflag": "3", # qfq
                    "source": self.source_tag,
                    "temporary": False
                }
            docs.append(doc)
            
        if not docs:
            return [], ""
            
        ops = []
        for d in docs:
            query = {"code": d["code"], "date": d["date"]}
            if freq == "5":
                query["time"] = d["time"]
            ops.append(UpdateOne(query, {"$set": d}, upsert=True))
            
        if ops:
            try:
                # Using batch of 2000
                for i in range(0, len(ops), 2000):
                    collection.bulk_write(ops[i:i+2000], ordered=False)
            except BulkWriteError as e:
                logger.error(f"Bulk write error on {code} {freq}: {e.details}")
                
        return docs, last_date

    # ------------------------------------------------------------------ #
    # High-Efficiency Bulk Intraday Snapshot Polling
    # ------------------------------------------------------------------ #
    def _is_trading_time(self, now: datetime) -> bool:
        if now.weekday() not in TRADING_DAYS:
            return False
        current = now.strftime("%H:%M")
        return any(start <= current < end for start, end in TRADING_PERIODS)

    def sync_once(self, current_time: Optional[datetime] = None) -> None:
        """Fetch bulk spot once and synthesize partial daily/minute klines."""
        now = current_time or datetime.now(BEIJING_TZ)
        df = pd.DataFrame()
        try:
            df = ak.stock_zh_a_spot_em()
        except Exception as e:
            logger.error(f"Failed to fetch market snapshot: {e}")
            return
            
        if df.empty:
            return
            
        trade_date = now.strftime(DATE_FORMAT)
        trade_time_min = now.strftime("%H:%M:00")
        
        daily_docs = []
        # Build K-line rows. For intraday, we just overwrite daily
        # Note: Since EastMoney spot contains current daily info directly (open, high, low, close = latest)
        
        for _, row in df.iterrows():
            code = str(row["代码"])
            if not code.startswith(("3", "0", "6", "4", "8")):
                continue
                
            latest = float(row.get("最新价", 0))
            if pd.isna(latest) or latest == 0.0:
                continue
                
            doc = {
                "code": code,
                "date": trade_date,
                "open": float(row.get("今开", latest)),
                "high": float(row.get("最高", latest)),
                "low": float(row.get("最低", latest)),
                "close": latest,
                "volume": float(row.get("成交量", 0)),
                "amount": float(row.get("成交额", 0)),
                "turn": float(row.get("换手率", 0)),
                "pctChg": float(row.get("涨跌幅", 0)),
                "adjustflag": "3",  # Best effort qfq assumption intraday
                "source": self.source_tag,
                "temporary": True  # Will be overwritten at EOD
            }
            daily_docs.append(doc)
            
        if daily_docs:
            ops = [UpdateOne({"code": d["code"], "date": d["date"]}, {"$set": d}, upsert=True) for d in daily_docs]
            for i in range(0, len(ops), 5000):
                self.daily_col.bulk_write(ops[i:i+5000], ordered=False)
                
            if self.backend_client:
                # Push intraday snapshot
                self.backend_client.push_kline(daily_docs, "d")
                
        logger.info(f"Intraday Snapshot processed: {len(daily_docs)} symbols at {now.strftime('%H:%M:%S')}")

    def run_loop(self, interval_minutes: int = 30, indicator_trigger: Optional[Callable] = None) -> None:
        """Run infinite loop for polling spot API."""
        logger.info(f"Entering continuous polling mode. Interval: {interval_minutes} mins.")
        try:
            while True:
                now = datetime.now(BEIJING_TZ)
                if self._is_trading_time(now):
                    self.sync_once(now)
                    if indicator_trigger:
                        logger.info("Triggering real-time indicators...")
                        indicator_trigger()
                    
                # Sleep interval
                time.sleep(interval_minutes * 60)
        except KeyboardInterrupt:
            logger.info("Manual termination of continuous polling.")

    # ------------------------------------------------------------------ #
    # EOD Validation & Clean
    # ------------------------------------------------------------------ #
    def run_validation(self, frequencies: Sequence[str] = None) -> None:
        """Fetch real EOD history for missing data today to overwrite temporary intraday bars."""
        logger.info("Running EOD Validation...")
        # To just get today's data, we set start_date=today and end_date=today
        today_str = datetime.now().strftime(DATE_FORMAT)
        self.sync_k_data(frequencies=frequencies, full_update=False, lookback_years=0, resume=False)

    def _get_collection_meta(self, frequency: str) -> Dict[str, Any]:
        if frequency not in self.collection_meta:
            raise ValueError(f"Unsupported frequency {frequency}")
        return self.collection_meta[frequency]
