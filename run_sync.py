from __future__ import annotations

import argparse
import json
import os

from src.config import load_runtime_config
from src.data.raw_sync import RawDataSyncService


DEFAULT_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "config.yaml",
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="One-shot local raw data maintenance for AStockDataSync",
    )
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="Path to config.yaml")
    parser.add_argument(
        "--frequency",
        dest="frequencies",
        action="append",
        choices=["d", "w", "m", "5"],
        default=None,
        help="Frequency to sync. Repeatable.",
    )
    parser.add_argument(
        "--daily-days",
        type=int,
        default=180,
        help="Lookback window for daily data",
    )
    parser.add_argument(
        "--symbol",
        dest="symbols",
        action="append",
        default=None,
        help="Specific symbol to maintain",
    )
    args = parser.parse_args()

    config = load_runtime_config(args.config)
    service = RawDataSyncService(config)
    service.initialize()
    try:
        astock_cfg = config.get("astock", {}) or {}
        frequencies = args.frequencies or astock_cfg.get("frequencies") or ["d"]
        result = service.maintain_database(
            frequencies=frequencies,
            symbols=args.symbols,
            daily_days=args.daily_days,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    finally:
        service.close()


if __name__ == "__main__":
    main()
