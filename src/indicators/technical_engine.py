from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence

import pandas as pd
from pymongo import ASCENDING, DESCENDING, MongoClient, UpdateOne
from pymongo.collection import Collection

from ..utils.config_loader import load_config


DATE_FMT = "%Y-%m-%d"


@dataclass
class IndicatorJob:
    name: str
    type: str
    symbol: str
    frequency: str = "d"
    params: Dict[str, Any] = field(default_factory=dict)
    timeframe: str = "1d"


class IndicatorEngine:
    """Lightweight indicator calculator that writes results back to MongoDB."""

    def __init__(self, config_path: str = "config.yaml", collection_name: Optional[str] = None, backend_client: Optional[Any] = None) -> None:
        self.config = load_config(config_path)
        self.backend_client = backend_client
        mongo_cfg = self.config["mongodb"]
        baostock_cfg = self.config["baostock"]

        self.client = MongoClient(mongo_cfg["uri"])
        self.db = self.client[baostock_cfg["db"]]
        indicator_collection = collection_name or baostock_cfg.get("indicator_collection", "indicator_data")
        self.indicator_col = self.db[indicator_collection]
        self.kline_collections: Dict[str, Collection] = {
            "d": self.db[baostock_cfg["daily"]],
            "w": self.db[baostock_cfg.get("weekly", "weekly_adjusted")],
            "m": self.db[baostock_cfg.get("monthly", "monthly_adjusted")],
            "5": self.db[baostock_cfg["minute_5"]],
        }
        self._ensure_indexes()
        self._handlers = {"macd": self._run_macd_job}

    def _ensure_indexes(self) -> None:
        self.indicator_col.create_index(
            [("indicator", ASCENDING), ("symbol", ASCENDING), ("timeframe", ASCENDING), ("timestamp", ASCENDING)],
            unique=True,
            name="indicator_symbol_timeframe_ts_idx",
        )

    def run_jobs(self, jobs: Sequence[Dict[str, Any]]) -> None:
        for raw in jobs:
            job = self._normalize_job(raw)
            if not job:
                continue
            handler = self._handlers.get(job.type.lower())
            if not handler:
                print(f"Unsupported indicator type: {job.type}")
                continue
            handler(job)

    def _normalize_job(self, raw: Dict[str, Any]) -> Optional[IndicatorJob]:
        if not raw:
            return None
        name = str(raw.get("name") or raw.get("type") or "").strip()
        job_type = str(raw.get("type") or "").strip().lower()
        symbol = str(raw.get("symbol") or "").strip()
        frequency = str(raw.get("frequency") or "d").strip().lower()
        if not name or not job_type or not symbol:
            return None
        timeframe = self._frequency_to_timeframe(frequency)
        return IndicatorJob(
            name=name,
            type=job_type,
            symbol=symbol,
            frequency=frequency,
            params=raw.get("params") or {},
            timeframe=timeframe,
        )

    def _run_macd_job(self, job: IndicatorJob) -> None:
        params = job.params or {}
        fast = int(params.get("fast", 12) or 12)
        slow = int(params.get("slow", 26) or 26)
        signal = int(params.get("signal", 9) or 9)

        last_ts = self._latest_indicator_timestamp(job)
        buffer_days = max(slow, signal) * 3
        series = self._load_price_series(job, backfill_days=buffer_days, since=last_ts)
        if series is None or series.empty:
            print(f"[MACD] No price series found for {job.symbol} ({job.frequency}).")
            return

        series = series.sort_values("date").reset_index(drop=True)
        close = series["close"]
        ema_fast = close.ewm(span=fast, adjust=False).mean()
        ema_slow = close.ewm(span=slow, adjust=False).mean()
        macd = ema_fast - ema_slow
        signal_line = macd.ewm(span=signal, adjust=False).mean()
        hist = macd - signal_line

        series["macd"] = macd
        series["signal"] = signal_line
        series["hist"] = hist

        if last_ts:
            series = series[series["date"] > last_ts]
        if series.empty:
            print(f"[MACD] {job.symbol} up-to-date; nothing to write.")
            return

        operations: List[UpdateOne] = []
        backend_records: List[Dict[str, Any]] = []
        for _, row in series.iterrows():
            dt_value: datetime = row["date"]
            payload = {
                "indicator": job.name,
                "symbol": job.symbol,
                "timeframe": job.timeframe,
                "timestamp": dt_value.isoformat(),
                "value": self._safe_float(row.get("hist")),
                "values": {
                    "macd": self._safe_float(row.get("macd")),
                    "signal": self._safe_float(row.get("signal")),
                    "hist": self._safe_float(row.get("hist")),
                    "fast": fast,
                    "slow": slow,
                    "signal_period": signal,
                },
                "payload": {"frequency": job.frequency, "source": "baostock"},
                "tags": ["technical", "macd"],
            }
            backend_records.append(payload)
            operations.append(
                UpdateOne(
                    {
                        "indicator": payload["indicator"],
                        "symbol": payload["symbol"],
                        "timeframe": payload["timeframe"],
                        "timestamp": payload["timestamp"],
                    },
                    {"$set": payload, "$setOnInsert": {"created_at": datetime.utcnow()}},
                    upsert=True,
                )
            )

        if not operations:
            print(f"[MACD] No operations prepared for {job.symbol}.")
            return

        self.indicator_col.bulk_write(operations, ordered=False)
        print(f"[MACD] {job.symbol} wrote {len(operations)} rows into {self.indicator_col.name}.")
        
        if self.backend_client:
            self.backend_client.push_indicators(backend_records)

    def _load_price_series(
        self,
        job: IndicatorJob,
        backfill_days: int = 0,
        since: Optional[datetime] = None,
    ) -> Optional[pd.DataFrame]:
        collection = self.kline_collections.get(job.frequency)
        if collection is None:
            print(f"No k-line collection found for frequency {job.frequency}; skip {job.name}.")
            return None

        query: Dict[str, Any] = {"code": job.symbol}
        if since:
            start_date = since.date() - timedelta(days=backfill_days)
            query["date"] = {"$gte": start_date.strftime(DATE_FMT)}

        cursor = collection.find(query, {"date": 1, "close": 1})
        cursor = cursor.sort([("date", ASCENDING)])
        rows = list(cursor)
        if not rows:
            return None

        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df = df.dropna(subset=["date", "close"])
        return df

    def _latest_indicator_timestamp(self, job: IndicatorJob) -> Optional[datetime]:
        record = self.indicator_col.find_one(
            {"indicator": job.name, "symbol": job.symbol, "timeframe": job.timeframe},
            {"timestamp": 1},
            sort=[("timestamp", DESCENDING)],
        )
        if not record or not record.get("timestamp"):
            return None
        try:
            return datetime.fromisoformat(str(record["timestamp"]))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        if pd.isna(number):
            return None
        return number

    @staticmethod
    def _frequency_to_timeframe(freq: str) -> str:
        mapping = {
            "d": "1d",
            "w": "1w",
            "m": "1m",
            "5": "5m",
        }
        return mapping.get(freq, freq)

    def close(self) -> None:
        self.client.close()

    def __enter__(self) -> "IndicatorEngine":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


__all__ = ["IndicatorEngine", "IndicatorJob"]
