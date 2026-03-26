from __future__ import annotations

import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence

import akshare as ak
from tqdm.auto import tqdm

from .cleaners import (
    clean_fundamental_snapshot,
    clean_kline,
    clean_stock_basic,
    chunked,
    market_prefixed_symbol,
    raw_symbol,
)
from .mongo_storage import MongoCollections, MongoStorage

logger = logging.getLogger(__name__)

MINUTE_FREQUENCIES = {"1", "5", "15", "30", "60"}
BAR_FREQUENCIES = {"d", "w", "m"} | MINUTE_FREQUENCIES


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


@dataclass(frozen=True)
class FetchPlan:
    symbol: str
    frequency: str
    start_at: datetime
    end_at: datetime
    latest_timestamp: Optional[datetime]


class RawDataSyncService:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self._disable_proxy()
        self._disable_akshare_progress()

        mongo_cfg = config.get("mongodb", {}) or {}
        collection_cfg = mongo_cfg.get("collections", {}) or {}
        self.storage = MongoStorage(
            mongo_cfg.get("uri", "mongodb://localhost:27017/"),
            mongo_cfg.get("database", "stock_platform"),
            collections=MongoCollections(
                stock_basic=collection_cfg.get("stock_basic", "stock_basic"),
                stock_kline=collection_cfg.get("stock_kline", "stock_kline"),
                stock_fundamental=collection_cfg.get(
                    "stock_fundamental", "stock_fundamental"
                ),
                sync_meta=collection_cfg.get("sync_meta", "sync_meta"),
            ),
        )

        astock_cfg = config.get("astock", {}) or {}
        self.batch_size = int(astock_cfg.get("batch_size", 500))
        self.pause_seconds = float(astock_cfg.get("pause_seconds", 0.2))
        self.symbol_batch_size = int(astock_cfg.get("symbol_batch_size", 30))
        self.batch_pause_seconds = float(astock_cfg.get("batch_pause_seconds", 2.0))
        self.request_workers = max(1, int(astock_cfg.get("request_workers", 2)))
        self.daily_lookback_days = int(astock_cfg.get("daily_lookback_days", 365))
        self.minute_lookback_days = int(astock_cfg.get("minute_lookback_days", 5))
        self.daily_retention_days = int(astock_cfg.get("daily_retention_days", 365))
        self.minute_retention_days = int(astock_cfg.get("minute_retention_days", 30))
        self.daily_overlap_days = int(astock_cfg.get("daily_overlap_days", 5))
        self.minute_overlap_minutes = int(astock_cfg.get("minute_overlap_minutes", 30))
        self.fundamental_report_periods = int(
            astock_cfg.get("fundamental_report_periods", 8)
        )
        self.fundamental_maintain_periods = int(
            astock_cfg.get("fundamental_maintain_periods", 1)
        )
        self.fundamental_refresh_hours = int(
            astock_cfg.get("fundamental_refresh_hours", 24)
        )

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
        for batch in tqdm(
            list(chunked(records, self.batch_size)),
            desc="[stock_basic] write",
            unit="batch",
            dynamic_ncols=True,
        ):
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

    def sync_fundamentals(
        self,
        *,
        report_dates: Optional[Sequence[str]] = None,
        periods: Optional[int] = None,
    ) -> SyncSummary:
        target_report_dates = list(report_dates or self._recent_report_dates(
            periods or self.fundamental_report_periods
        ))
        summary = SyncSummary()
        for report_date in tqdm(
            target_report_dates,
            desc="[stock_fundamental] reports",
            unit="report",
            dynamic_ncols=True,
        ):
            if self._should_skip_fundamental_report(report_date):
                logger.info("Skip stock fundamentals for report_date=%s", report_date)
                continue
            logger.info("Fetching stock fundamentals for report_date=%s", report_date)
            frame = ak.stock_yjbb_em(date=report_date)
            records = clean_fundamental_snapshot(
                frame,
                report_date=report_date,
                source="yjbb",
            )
            summary.total += len(records)
            for batch in chunked(records, self.batch_size):
                summary.merge(self.storage.upsert_stock_fundamental(batch))
            self.storage.update_sync_meta(
                "stock_fundamental",
                report_date,
                {
                    "report_date": report_date,
                    "record_count": len(records),
                    "last_success_at": datetime.utcnow(),
                },
            )
        logger.info("Fundamental sync finished: %s", summary.__dict__)
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
        if normalized_frequency not in BAR_FREQUENCIES:
            raise ValueError(f"Unsupported frequency: {normalized_frequency}")

        sync_symbols = list(symbols or self.storage.list_symbols(limit=limit))
        if limit:
            sync_symbols = sync_symbols[:limit]
        if not sync_symbols:
            raise ValueError("No symbols available. Run stock basic sync first.")
        if normalized_frequency in MINUTE_FREQUENCIES:
            self._ensure_minute_source_available(
                symbol=sync_symbols[0],
                frequency=normalized_frequency,
            )

        lookback_days = days or self._resolve_days(normalized_frequency)
        latest_map = self.storage.get_latest_kline_timestamps(
            symbols=sync_symbols,
            frequency=normalized_frequency,
        )
        plans = [
            self._build_fetch_plan(
                symbol=symbol,
                frequency=normalized_frequency,
                latest_timestamp=latest_map.get(symbol),
                lookback_days=lookback_days,
            )
            for symbol in sync_symbols
        ]

        summary = SyncSummary()
        progress = tqdm(
            total=len(plans),
            desc=f"[stock_kline:{normalized_frequency}] symbols",
            unit="symbol",
            dynamic_ncols=True,
        )
        try:
            for batch_index, plan_batch in enumerate(
                chunked(plans, self.symbol_batch_size),
                start=1,
            ):
                logger.info(
                    "Syncing batch %s for frequency=%s, size=%s",
                    batch_index,
                    normalized_frequency,
                    len(plan_batch),
                )
                batch_summary = self._sync_plan_batch(plan_batch, progress=progress)
                summary.total += batch_summary.total
                summary.merge(batch_summary.__dict__)
                if self.batch_pause_seconds > 0:
                    time.sleep(self.batch_pause_seconds)
        finally:
            progress.close()

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
        module_progress = tqdm(
            total=2 + len(frequencies),
            desc="[maintain] modules",
            unit="module",
            dynamic_ncols=True,
        )
        try:
            results["stock_basic"] = self.sync_stock_basic().__dict__.copy()
            module_progress.update(1)
            results["stock_fundamental"] = self.sync_fundamentals(
                periods=self.fundamental_maintain_periods
            ).__dict__.copy()
            module_progress.update(1)
            for frequency in frequencies:
                summary = self.sync_kline(
                    symbols=symbols,
                    frequency=frequency,
                    days=self._resolve_days(frequency, daily_days=daily_days),
                )
                results[f"stock_kline:{frequency}"] = summary.__dict__.copy()
                pruned = self._prune_old_kline(frequency)
                if pruned:
                    results[f"stock_kline:{frequency}"]["pruned"] = pruned
                module_progress.update(1)
        finally:
            module_progress.close()
        return results

    def _sync_plan_batch(self, plans: List[FetchPlan], *, progress) -> SyncSummary:
        summary = SyncSummary()
        with ThreadPoolExecutor(max_workers=self.request_workers) as executor:
            future_map = {
                executor.submit(self._fetch_plan_records, plan): plan
                for plan in plans
            }
            for future in as_completed(future_map):
                plan = future_map[future]
                try:
                    records = future.result()
                except Exception as exc:
                    logger.warning(
                        "Failed to sync %s frequency=%s: %s",
                        plan.symbol,
                        plan.frequency,
                        exc,
                    )
                    progress.update(1)
                    continue

                if not records:
                    progress.update(1)
                    continue

                summary.total += len(records)
                for write_batch in chunked(records, self.batch_size):
                    summary.merge(self.storage.upsert_stock_kline(write_batch))

                latest_timestamp = max(record["timestamp"] for record in records)
                self.storage.update_sync_meta(
                    "stock_kline",
                    f"{plan.symbol}:{plan.frequency}",
                    {
                        "symbol": plan.symbol,
                        "frequency": plan.frequency,
                        "records_synced": len(records),
                        "last_trade_timestamp": latest_timestamp,
                        "last_success_at": datetime.utcnow(),
                    },
                )
                if self.pause_seconds > 0:
                    time.sleep(self.pause_seconds)
                progress.update(1)
        return summary

    def _build_fetch_plan(
        self,
        *,
        symbol: str,
        frequency: str,
        latest_timestamp: Optional[datetime],
        lookback_days: int,
    ) -> FetchPlan:
        end_at = datetime.now()
        if latest_timestamp is None:
            start_at = end_at - timedelta(days=lookback_days)
        elif frequency in MINUTE_FREQUENCIES:
            start_at = latest_timestamp - timedelta(minutes=self.minute_overlap_minutes)
        else:
            start_at = latest_timestamp - timedelta(days=self.daily_overlap_days)

        return FetchPlan(
            symbol=symbol,
            frequency=frequency,
            start_at=start_at,
            end_at=end_at,
            latest_timestamp=latest_timestamp,
        )

    def _ensure_minute_source_available(self, *, symbol: str, frequency: str) -> None:
        probe_plan = self._build_fetch_plan(
            symbol=symbol,
            frequency=frequency,
            latest_timestamp=None,
            lookback_days=1,
        )
        try:
            self._fetch_minute_records(probe_plan)
        except Exception as exc:
            raise RuntimeError(
                f"Minute data source unavailable for frequency={frequency}: {exc}"
            ) from exc

    def _fetch_plan_records(self, plan: FetchPlan) -> List[Dict[str, Any]]:
        if self._should_skip_plan(plan):
            logger.info(
                "Skip unsupported symbol %s for frequency=%s",
                plan.symbol,
                plan.frequency,
            )
            return []
        if plan.frequency == "d":
            records = self._fetch_daily_records(plan)
        elif plan.frequency in {"w", "m"}:
            records = self._fetch_bar_records(plan)
        elif plan.frequency in MINUTE_FREQUENCIES:
            records = self._fetch_minute_records(plan)
        else:
            raise ValueError(f"Unsupported frequency: {plan.frequency}")

        if plan.latest_timestamp is None:
            return records
        return [
            record
            for record in records
            if record["timestamp"] > plan.latest_timestamp
        ]

    def _fetch_daily_records(self, plan: FetchPlan) -> List[Dict[str, Any]]:
        df = ak.stock_zh_a_hist_tx(
            symbol=market_prefixed_symbol(plan.symbol),
            start_date=plan.start_at.strftime("%Y%m%d"),
            end_date=plan.end_at.strftime("%Y%m%d"),
            adjust="qfq",
        )
        return clean_kline(plan.symbol, plan.frequency, df)

    def _fetch_bar_records(self, plan: FetchPlan) -> List[Dict[str, Any]]:
        df = ak.stock_zh_a_hist(
            symbol=raw_symbol(plan.symbol),
            period={"w": "weekly", "m": "monthly"}[plan.frequency],
            start_date=plan.start_at.strftime("%Y%m%d"),
            end_date=plan.end_at.strftime("%Y%m%d"),
            adjust="qfq",
        )
        return clean_kline(plan.symbol, plan.frequency, df)

    def _fetch_minute_records(self, plan: FetchPlan) -> List[Dict[str, Any]]:
        df = ak.stock_zh_a_hist_min_em(
            symbol=raw_symbol(plan.symbol),
            start_date=plan.start_at.strftime("%Y-%m-%d %H:%M:%S"),
            end_date=plan.end_at.strftime("%Y-%m-%d %H:%M:%S"),
            period=plan.frequency,
            adjust="",
        )
        if not df.empty:
            time_column = None
            if "时间" in df.columns:
                time_column = "时间"
            elif "鏃堕棿" in df.columns:
                time_column = "鏃堕棿"
            if time_column:
                df = df[df[time_column] >= plan.start_at.strftime("%Y-%m-%d %H:%M:%S")]
        return clean_kline(plan.symbol, plan.frequency, df)

    def _recent_report_dates(self, periods: int) -> List[str]:
        if periods <= 0:
            return []

        now = datetime.now()
        quarter_ends = ((3, 31), (6, 30), (9, 30), (12, 31))
        cursor_year = now.year
        report_dates: List[str] = []

        while len(report_dates) < periods:
            for month, day in reversed(quarter_ends):
                candidate = datetime(cursor_year, month, day)
                if candidate <= now:
                    report_dates.append(candidate.strftime("%Y%m%d"))
                    if len(report_dates) >= periods:
                        break
            cursor_year -= 1

        return report_dates

    def _should_skip_fundamental_report(self, report_date: str) -> bool:
        meta = self.storage.get_sync_meta("stock_fundamental", report_date)
        if not meta:
            return False
        last_success_at = meta.get("last_success_at")
        if not isinstance(last_success_at, datetime):
            return False
        refresh_after = last_success_at + timedelta(hours=self.fundamental_refresh_hours)
        return refresh_after > datetime.utcnow()

    @staticmethod
    def _should_skip_plan(plan: FetchPlan) -> bool:
        raw_code = raw_symbol(plan.symbol)
        if plan.frequency == "d" and (
            plan.symbol.startswith("BJ")
            or raw_code.startswith(("4", "8", "9"))
        ):
            return True
        return False

    def _resolve_days(self, frequency: str, daily_days: int = 180) -> int:
        if frequency in MINUTE_FREQUENCIES:
            return self.minute_lookback_days
        if frequency == "w":
            return max(365, daily_days)
        if frequency == "m":
            return max(365 * 3, daily_days)
        return daily_days

    def _prune_old_kline(self, frequency: str) -> int:
        now = datetime.now()
        if frequency in MINUTE_FREQUENCIES:
            cutoff = now - timedelta(days=self.minute_retention_days)
        elif frequency == "d":
            cutoff = now - timedelta(days=self.daily_retention_days)
        else:
            return 0
        deleted = self.storage.delete_kline_older_than(
            frequency=frequency,
            cutoff=cutoff,
        )
        if deleted:
            logger.info(
                "Pruned %s old records for frequency=%s before %s",
                deleted,
                frequency,
                cutoff.isoformat(),
            )
        return deleted

    @staticmethod
    def _disable_proxy() -> None:
        for key in (
            "http_proxy",
            "https_proxy",
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "all_proxy",
            "ALL_PROXY",
        ):
            os.environ.pop(key, None)
        os.environ["NO_PROXY"] = "*"
        os.environ["no_proxy"] = "*"

    @staticmethod
    def _disable_akshare_progress() -> None:
        silent_tqdm = lambda enable=True: (  # noqa: E731
            lambda iterable, *args, **kwargs: iterable
        )
        for name, module in list(sys.modules.items()):
            if not name.startswith("akshare"):
                continue
            if hasattr(module, "get_tqdm"):
                setattr(module, "get_tqdm", silent_tqdm)
