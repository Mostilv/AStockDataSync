from __future__ import annotations

import argparse
import json
import logging
from typing import Sequence

from .config import load_runtime_config
from .data.cleaners import normalize_symbol
from .data.raw_sync import RawDataSyncService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="AStock raw data sync and local database maintenance",
    )
    parser.add_argument("--config", default=None, help="Path to config.yaml")

    subparsers = parser.add_subparsers(dest="command", required=True)

    basic_parser = subparsers.add_parser("basic", help="Sync stock basic data")
    basic_parser.set_defaults(handler=handle_basic)

    fundamental_parser = subparsers.add_parser(
        "fundamental",
        help="Sync market-wide fundamental snapshots by report date",
    )
    fundamental_parser.add_argument(
        "--report-date",
        dest="report_dates",
        action="append",
        default=None,
        help="Quarter report date such as 20241231. Repeatable.",
    )
    fundamental_parser.add_argument(
        "--periods",
        type=int,
        default=None,
        help="How many recent report periods to sync when report dates are omitted.",
    )
    fundamental_parser.set_defaults(handler=handle_fundamental)

    fundamental_latest_parser = subparsers.add_parser(
        "fundamental-latest",
        help="Sync only the latest report-period fundamentals",
    )
    fundamental_latest_parser.set_defaults(handler=handle_fundamental_latest)

    kline_parser = subparsers.add_parser("kline", help="Sync K-line data")
    kline_parser.add_argument(
        "--frequency",
        default="d",
        choices=["d", "w", "m", "1", "5", "15", "30", "60"],
        help="K-line frequency",
    )
    kline_parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="Lookback days for daily/weekly/monthly data",
    )
    kline_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of symbols to sync",
    )
    kline_parser.add_argument(
        "--symbol",
        dest="symbols",
        action="append",
        default=None,
        help="Specific symbol to sync, e.g. SH600519 or 600519",
    )
    kline_parser.set_defaults(handler=handle_kline)

    maintain_parser = subparsers.add_parser(
        "maintain",
        help="Sync basic data and selected K-line datasets into MongoDB",
    )
    maintain_parser.add_argument(
        "--frequency",
        dest="frequencies",
        action="append",
        choices=["d", "w", "m", "1", "5", "15", "30", "60"],
        default=None,
        help="Frequency to maintain. Repeatable. Defaults to config values.",
    )
    maintain_parser.add_argument(
        "--daily-days",
        type=int,
        default=180,
        help="Lookback window for daily data",
    )
    maintain_parser.add_argument(
        "--symbol",
        dest="symbols",
        action="append",
        default=None,
        help="Specific symbol to sync",
    )
    maintain_parser.set_defaults(handler=handle_maintain)

    return parser


def handle_basic(args: argparse.Namespace) -> None:
    config = load_runtime_config(args.config)
    service = RawDataSyncService(config)
    service.initialize()
    try:
        summary = service.sync_stock_basic()
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2, default=str))
    finally:
        service.close()


def handle_fundamental(args: argparse.Namespace) -> None:
    config = load_runtime_config(args.config)
    service = RawDataSyncService(config)
    service.initialize()
    try:
        summary = service.sync_fundamentals(
            report_dates=args.report_dates,
            periods=args.periods,
        )
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2, default=str))
    finally:
        service.close()


def handle_fundamental_latest(args: argparse.Namespace) -> None:
    config = load_runtime_config(args.config)
    service = RawDataSyncService(config)
    service.initialize()
    try:
        summary = service.sync_fundamentals(periods=1)
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2, default=str))
    finally:
        service.close()


def handle_kline(args: argparse.Namespace) -> None:
    config = load_runtime_config(args.config)
    service = RawDataSyncService(config)
    service.initialize()
    try:
        summary = service.sync_kline(
            symbols=_normalize_symbols(args.symbols),
            frequency=args.frequency,
            days=args.days,
            limit=args.limit,
        )
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2, default=str))
    finally:
        service.close()


def handle_maintain(args: argparse.Namespace) -> None:
    config = load_runtime_config(args.config)
    astock_cfg = config.get("astock", {}) or {}
    frequencies = args.frequencies or astock_cfg.get("frequencies") or ["d"]
    service = RawDataSyncService(config)
    service.initialize()
    try:
        results = service.maintain_database(
            frequencies=frequencies,
            symbols=_normalize_symbols(args.symbols),
            daily_days=args.daily_days,
        )
        print(json.dumps(results, ensure_ascii=False, indent=2, default=str))
    finally:
        service.close()


def _normalize_symbols(values: Sequence[str] | None) -> list[str] | None:
    if not values:
        return None
    return [normalize_symbol(item) for item in values if item]


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.handler(args)


if __name__ == "__main__":
    main()
