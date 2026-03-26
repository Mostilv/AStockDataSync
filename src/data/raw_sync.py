from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence

import akshare as ak

from .cleaners import clean_kline, clean_stock_basic, chunked, raw_symbol
from .mongo_storage import MongoCollections, MongoStorage

logger = logging.getLogger(__name__)


@dataclass
class SyncSummary:
    total: int = 0
    matched: int = 0
    modified: int = 0
    upserted: int = 0

    def merge(self, payload: Dict[str, int]) -> None:
        self.matched += int(payload.get("matched", 0))
        self.modified += int(payload.get("modified", 0))
        self.upserted += int(payload.get("upserted", 0))


class RawDataSyncService:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        mongo_cfg = config.get("mongodb", {}) or {}
        collection_cfg = (mongo_cfg.get("collections", {}) or {})
        self.storage = MongoStorage(
            mongo_cfg.get("uri", "mongodb://localhost:27017/"),
            mongo_cfg.get("database", "stock_platform"),
            collections=MongoCollections(
                stock_basic=collection_cfg.get("stock_basic", "stock_basic"),
                stock_kline=collection_cfg.get("stock_kline", "stock_kline"),
                sync_meta=collection_cfg.get("sync_meta", "sync_meta"),
            ),
        )
        self.astock_cfg = config.get("astock", {}) or {}
        self.batch_size = int(self.astock_cfg.get("batch_size", 500))
        self.pause_seconds = float(self.astock_cfg.get("pause_seconds", 0.2))

    def initialize(self) -> None:
        self.storage.ping()
        self.storage.ensure_indexes()

    def close(self) -> None:
        self.storage.close()

    def sync_stock_basic(self) -> SyncSummary:
        logger.info("Fetching stock basic data from akshare")
        summary = SyncSummary()
        basic_df = ak.stock_info_a_code_name()
        records = clean_stock_basic(basic_df)
        summary.total = len(records)
        for batch in chunked(records, self.batch_size):
            summary.merge(self.storage.upsert_stock_basic(batch))
        self.storage.update_sync_meta(
            "stock_basic",
            "all",
            {
                "record_count": summary.total,
                "last_success_at": datetime.utcnow(),
            },
        )
        logger.info("Stock basic sync finished: %s", summary.__dict__)
        return summary

    def sync_kline(
        self,
        *,
        symbols: Optional[Sequence[str]] = None,
        frequency: str = "d",
        days: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> SyncSummary:
        normalized_frequency = str(frequency).strip().lower()
        sync_symbols = list(symbols or self.storage.list_symbols(limit=limit))
        if limit:
            sync_symbols = sync_symbols[:limit]

        if not sync_symbols:
            raise ValueError("No symbols available. Run stock basic sync first.")

        summary = SyncSummary(total=0)
        for index, symbol in enumerate(sync_symbols, start=1):
            logger.info("Syncing %s (%s/%s) frequency=%s", symbol, index, len(sync_symbols), normalized_frequency)
            try:
                records = self._fetch_symbol_kline(symbol, normalized_frequency, days=days)
            except Exception as exc:
                logger.warning("Failed to sync %s: %s", symbol, exc)
                continue
            if not records:
                continue
            summary.total += len(records)
            for batch in chunked(records, self.batch_size):
                summary.merge(self.storage.upsert_stock_kline(batch))
            latest_timestamp = max(record["timestamp"] for record in records)
            self.storage.update_sync_meta(
                "stock_kline",
                f"{symbol}:{normalized_frequency}",
                {
                    "symbol": symbol,
                    "frequency": normalized_frequency,
                    "records_synced": len(records),
                    "last_trade_timestamp": latest_timestamp,
                    "last_success_at": datetime.utcnow(),
                },
            )
            time.sleep(self.pause_seconds)

        logger.info("K-line sync finished: %s", summary.__dict__)
        return summary

    def maintain_database(
        self,
        *,
        frequencies: Sequence[str],
        symbols: Optional[Sequence[str]] = None,
        daily_days: int = 180,
    ) -> Dict[str, Dict[str, int]]:
        results: Dict[str, Dict[str, int]] = {}
        basic_summary = self.sync_stock_basic()
        results["stock_basic"] = basic_summary.__dict__.copy()
        for frequency in frequencies:
            lookback_days = self._resolve_days(frequency, daily_days)
            summary = self.sync_kline(
                symbols=symbols,
                frequency=frequency,
                days=lookback_days,
            )
            results[f"stock_kline:{frequency}"] = summary.__dict__.copy()
        return results

    def _fetch_symbol_kline(
        self,
        symbol: str,
        frequency: str,
        *,
        days: Optional[int],
    ) -> List[Dict[str, Any]]:
        raw_code = raw_symbol(symbol)
        start_date = None
        end_date = datetime.now().strftime("%Y%m%d")
        if days and frequency in {"d", "w", "m"}:
            start_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")

        if frequency in {"d", "w", "m"}:
            period_map = {"d": "daily", "w": "weekly", "m": "monthly"}
            df = ak.stock_zh_a_hist(
                symbol=raw_code,
                period=period_map[frequency],
                start_date=start_date,
                end_date=end_date,
                adjust="qfq",
            )
            return clean_kline(symbol, frequency, df)

        if frequency == "5":
            df = ak.stock_zh_a_hist_min_em(
                symbol=raw_code,
                period="5",
                adjust="qfq",
            )
            if days:
                earliest = datetime.now() - timedelta(days=days)
                df = df[df["时间"] >= earliest.strftime("%Y-%m-%d %H:%M:%S")]
            return clean_kline(symbol, frequency, df)

        raise ValueError(f"Unsupported frequency: {frequency}")

    @staticmethod
    def _resolve_days(frequency: str, daily_days: int) -> int:
        if frequency == "5":
            return 10
        if frequency == "w":
            return max(365, daily_days)
        if frequency == "m":
            return max(365 * 3, daily_days)
        return daily_days
