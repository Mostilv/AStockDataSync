from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime

from src.config import load_runtime_config
from src.data.raw_sync import RawDataSyncService


DEFAULT_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "config.yaml",
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Periodic raw data maintenance loop for AStockDataSync",
    )
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="Path to config.yaml")
    parser.add_argument(
        "--interval-minutes",
        type=int,
        default=None,
        help="Override maintenance interval in minutes",
    )
    args = parser.parse_args()

    config = load_runtime_config(args.config)
    astock_cfg = config.get("astock", {}) or {}
    interval_minutes = args.interval_minutes or int(astock_cfg.get("maintain_interval_minutes", 60))
    frequencies = astock_cfg.get("frequencies") or ["d"]
    daily_days = int(astock_cfg.get("daily_lookback_days", 365))

    while True:
        started_at = datetime.utcnow()
        service = RawDataSyncService(config)
        service.initialize()
        try:
            result = service.maintain_database(
                frequencies=frequencies,
                daily_days=daily_days,
            )
            print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        finally:
            service.close()

        elapsed_seconds = int((datetime.utcnow() - started_at).total_seconds())
        print(f"[AStockDataSync] cycle finished in {elapsed_seconds}s, sleep {interval_minutes} minutes")
        time.sleep(interval_minutes * 60)


if __name__ == "__main__":
    main()
