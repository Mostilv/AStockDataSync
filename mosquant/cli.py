import argparse
import os
import time
from typing import Callable

os.environ["PYTHONIOENCODING"] = "utf-8"

from mosquant.data.manager_akshare import AkshareRealtimeManager
from mosquant.data.manager_baostock import BaostockManager
from mosquant.data.manager_tushare import TushareManager


def handle_baostock(args: argparse.Namespace) -> None:
    """Dispatch tasks that rely on the BaostockManager."""
    manager = BaostockManager(config_path=args.config)
    try:
        if args.action == "basic":
            manager.query_stock_basic(refresh=args.refresh)
        elif args.action == "kline":
            manager.sync_k_data(
                frequencies=args.frequencies,
                full_update=args.full,
                lookback_years=args.years,
            )
        else:
            raise ValueError(f"Unsupported Baostock action: {args.action}")
    finally:
        manager.close()


def handle_tushare(args: argparse.Namespace) -> None:
    """Dispatch tasks driven by the TushareManager."""
    manager = TushareManager(config_path=args.config)
    try:
        if args.action == "basic":
            manager.fetch_stock_basic()
        elif args.action == "daily":
            end_date = args.end_date or time.strftime("%Y%m%d", time.localtime())
            manager.fetch_all_daily_data(
                start_date=args.start_date,
                end_date=end_date,
                max_threads=args.max_threads,
            )
        elif args.action == "oneday":
            trade_date = args.trade_date or time.strftime("%Y%m%d", time.localtime())
            df = manager.fetch_one_day_data(trade_date=trade_date)
            if df.empty:
                print(f"No daily data fetched for {trade_date}.")
            else:
                manager.save_to_mongo(df)
                print(f"Saved {len(df)} rows for {trade_date}.")
        else:
            raise ValueError(f"Unsupported Tushare action: {args.action}")
    finally:
        manager.close()


def handle_akshare(args: argparse.Namespace) -> None:
    """Dispatch Akshare realtime routines."""
    manager = AkshareRealtimeManager(config_path=args.config)
    try:
        if args.action == "once":
            manager.sync_once(
                ignore_trading_window=args.ignore_hours,
                force_flush=args.force_flush,
            )
        elif args.action == "realtime":
            manager.run_loop(
                iterations=args.iterations,
                ignore_trading_window=args.ignore_hours,
            )
        else:
            raise ValueError(f"Unsupported Akshare action: {args.action}")
    finally:
        manager.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Data maintenance utilities for mosQuant (Baostock/Tushare)."
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to YAML config file (default: config.yaml)",
    )

    subparsers = parser.add_subparsers(dest="source", required=True)

    # Baostock commands
    baostock_parser = subparsers.add_parser("baostock", help="Tasks for Baostock data")
    baostock_parser.add_argument(
        "action",
        choices=["basic", "kline"],
        help="basic: refresh stock basics; kline: update K-line collections",
    )
    baostock_parser.add_argument(
        "--refresh",
        action="store_true",
        help="When used with 'basic', force refresh instead of incremental update.",
    )
    baostock_parser.add_argument(
        "--full",
        action="store_true",
        help="When used with 'kline', fetch data from START_DATE instead of last record.",
    )
    baostock_parser.add_argument(
        "--freq",
        dest="frequencies",
        action="append",
        choices=["d", "15", "60"],
        help="Specify frequencies (repeatable). Defaults to config frequencies.",
    )
    baostock_parser.add_argument(
        "--years",
        type=int,
        default=None,
        help="Custom lookback window when collection is empty. Default: config history_years.",
    )
    baostock_parser.set_defaults(handler=handle_baostock)

    # Tushare commands
    tushare_parser = subparsers.add_parser("tushare", help="Tasks for Tushare data")
    tushare_parser.add_argument(
        "action",
        choices=["basic", "daily", "oneday"],
        help=(
            "basic: refresh stock list; "
            "daily: fetch historical daily bars for all symbols; "
            "oneday: fetch a single trade date and persist it"
        ),
    )
    tushare_parser.add_argument(
        "--start-date",
        default="20150101",
        help="Start date for 'daily' (YYYYMMDD).",
    )
    tushare_parser.add_argument(
        "--end-date",
        default=None,
        help="End date for 'daily' (YYYYMMDD). Defaults to today.",
    )
    tushare_parser.add_argument(
        "--max-threads",
        type=int,
        default=30,
        help="Thread pool size for 'daily' fetch (default: 30).",
    )
    tushare_parser.add_argument(
        "--trade-date",
        default=None,
        help="Trade date for 'oneday' (YYYYMMDD). Defaults to today.",
    )
    tushare_parser.set_defaults(handler=handle_tushare)

    # Akshare commands
    akshare_parser = subparsers.add_parser("akshare", help="Tasks for Akshare realtime quotes")
    akshare_parser.add_argument(
        "action",
        choices=["once", "realtime"],
        help="once: fetch snapshot once; realtime: keep looping until interrupted.",
    )
    akshare_parser.add_argument(
        "--ignore-hours",
        action="store_true",
        help="Allow execution outside regular trading hours.",
    )
    akshare_parser.add_argument(
        "--force-flush",
        action="store_true",
        help="Force unfinished timeframe bars to be persisted after a single run.",
    )
    akshare_parser.add_argument(
        "--iterations",
        type=int,
        default=None,
        help="Limit realtime loop iterations (default: infinite).",
    )
    akshare_parser.set_defaults(handler=handle_akshare)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    handler: Callable[[argparse.Namespace], None] = getattr(args, "handler", None)

    if handler is None:
        parser.error("No handler registered for the selected command.")
    handler(args)


if __name__ == "__main__":
    main()
