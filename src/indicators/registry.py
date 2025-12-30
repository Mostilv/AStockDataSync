from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence

from pymongo import ASCENDING, MongoClient, UpdateOne
from pymongo.collection import Collection

from .industry_breadth import IndustryBreadthCalculator
from .industry_metrics import IndustryMetricsCollector
from .technical_engine import IndicatorEngine
from ..utils.config_loader import load_config
from ..utils.backend_client import BackendClient


def _ensure_indicator_index(collection: Collection) -> None:
    collection.create_index(
        [("indicator", ASCENDING), ("symbol", ASCENDING), ("timeframe", ASCENDING), ("timestamp", ASCENDING)],
        unique=True,
        name="indicator_symbol_timeframe_ts_idx",
    )


def _upsert_indicator_records(collection: Collection, records: Iterable[Dict[str, Any]], backend_client: Optional[BackendClient] = None) -> int:
    ops: List[UpdateOne] = []
    push_list: List[Dict[str, Any]] = []
    count = 0
    for doc in records:
        if not doc:
            continue
        key = {
            "indicator": doc.get("indicator"),
            "symbol": doc.get("symbol"),
            "timeframe": doc.get("timeframe"),
            "timestamp": doc.get("timestamp"),
        }
        if not all(key.values()):
            continue
        ops.append(
            UpdateOne(
                key,
                {"$set": doc, "$setOnInsert": {"created_at": datetime.utcnow()}},
                upsert=True,
            )
        )
        push_list.append(doc)
    
    if backend_client and push_list:
        backend_client.push_indicators(push_list)
        
    if not ops:
        return 0
    result = collection.bulk_write(ops, ordered=False)
    count = result.upserted_count + result.modified_count
    return count


def run_indicator_suite(
    config_path: str = "config.yaml",
    jobs: Optional[Sequence[Dict[str, Any]]] = None,
    dry_run: bool = False,
    backend_client: Optional[BackendClient] = None
) -> None:
    """
    Run all configured indicators after data sync:
    - Technical jobs (e.g., MACD) via IndicatorEngine
    - Industry metrics (momentum/width) via IndustryMetricsCollector
    - Industry breadth via IndustryBreadthCalculator
    """
    config = load_config(config_path)
    workflow_cfg = (config.get("workflow", {}) or {}).get("daily_update", {}) or {}
    indicator_cfg = workflow_cfg.get("indicators", {}) or {}
    if not indicator_cfg.get("enabled", True):
        return

    baostock_cfg = config.get("baostock", {}) or {}
    mongo_cfg = config.get("mongodb", {}) or {}
    indicator_collection_name = baostock_cfg.get("indicator_collection", "indicator_data")
    run_industry_metrics = indicator_cfg.get("run_industry_metrics", True)
    run_industry_breadth = indicator_cfg.get("run_industry_breadth", True)
    technical_jobs = list(jobs or indicator_cfg.get("jobs") or [])

    if dry_run:
        print(
            "[Dry Run] indicators -> "
            f"technical_jobs={len(technical_jobs)}, "
            f"industry_metrics={run_industry_metrics}, "
            f"industry_breadth={run_industry_breadth}, "
            f"collection={indicator_collection_name}"
        )
        return

    client = MongoClient(mongo_cfg.get("uri", "mongodb://localhost:27017/"))
    db = client.get_database(baostock_cfg.get("db", "baostock"))
    indicator_col = db[indicator_collection_name]
    _ensure_indicator_index(indicator_col)

    try:
        # 1) Technical indicators (e.g., MACD) driven by jobs
        if technical_jobs:
            engine = IndicatorEngine(config_path=config_path, collection_name=indicator_collection_name, backend_client=backend_client)
            try:
                engine.run_jobs(technical_jobs)
            finally:
                engine.close()

        # 2) Industry metrics (momentum/width)
        if run_industry_metrics:
            metrics_kwargs: Dict[str, Any] = {}
            if indicator_cfg.get("industry_metrics_lookback_days") is not None:
                metrics_kwargs["lookback_days"] = int(indicator_cfg.get("industry_metrics_lookback_days"))
            if indicator_cfg.get("industry_metrics_momentum") is not None:
                metrics_kwargs["momentum_period"] = int(indicator_cfg.get("industry_metrics_momentum"))
            if indicator_cfg.get("industry_limit") is not None:
                metrics_kwargs["industry_limit"] = int(indicator_cfg.get("industry_limit"))
            if indicator_cfg.get("industry_codes") is not None:
                metrics_kwargs["codes"] = indicator_cfg.get("industry_codes")

            collector = IndustryMetricsCollector(**metrics_kwargs)
            metric_records = collector.collect()
            if metric_records:
                written = _upsert_indicator_records(indicator_col, metric_records, backend_client=backend_client)
                print(f"[Indicator] industry_metrics upserted {written} records into {indicator_collection_name}.")
            else:
                print("[Indicator] industry_metrics produced no records; skipped.")

        # 3) Industry breadth (pct above MA)
        if run_industry_breadth:
            breadth = IndustryBreadthCalculator(
                config_path=config_path,
                indicator=indicator_cfg.get("breadth_indicator"),
                timeframe=indicator_cfg.get("breadth_timeframe", "1d"),
                lookback_days=indicator_cfg.get("breadth_lookback_days"),
                ma_window=indicator_cfg.get("breadth_ma_window"),
                collection_name=indicator_collection_name,
                save_local=False,
            )
            try:
                breadth_records = breadth.collect()
                if breadth_records:
                    written = _upsert_indicator_records(indicator_col, breadth_records, backend_client=backend_client)
                    print(f"[Indicator] industry_breadth upserted {written} records into {indicator_collection_name}.")
                else:
                    print("[Indicator] industry_breadth produced no records; skipped.")
            finally:
                breadth.close()
    finally:
        client.close()


__all__ = ["run_indicator_suite"]
