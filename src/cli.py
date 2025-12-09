import argparse
import os
import time
from typing import Callable

os.environ["PYTHONIOENCODING"] = "utf-8"

from .data.manager_akshare import AkshareRealtimeManager
from .data.manager_backend import StockMiddlePlatformBackendSync
from .data.manager_baostock import BaostockManager


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
                resume=args.resume,
            )
        elif args.action == "limitup-minutes":
            manager.sync_limit_up_minute_data(
                days=args.limitup_days,
                frequencies=tuple(args.minute_frequencies or ("5",)),
                pct_threshold=args.pct_threshold,
            )
        elif args.action == "finance":
            manager.sync_finance_data(
                full_update=args.full,
                years=args.years,
                resume=args.resume,
            )
        else:
            raise ValueError(f"Unsupported Baostock action: {args.action}")
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


def handle_backend(args: argparse.Namespace) -> None:
    """Send MongoDB data to stock_middle_platform_backend."""
    manager = StockMiddlePlatformBackendSync(config_path=args.config)
    try:
        if args.action == "basic":
            total = manager.push_stock_basic(
                batch_size=args.batch_size,
                limit=args.limit,
            )
            print(f"已推送 {total} 条股票基本信息。")
        elif args.action == "kline":
            total = manager.push_kline(
                frequency=args.frequency,
                start_date=args.start_date,
                end_date=args.end_date,
                batch_size=args.batch_size,
                limit=args.limit,
            )
            print(f"已推送 {total} 条 {args.frequency} 级别K线数据。")
        elif args.action == "indicators":
            total = manager.push_industry_metrics(
                lookback_days=args.metrics_window,
                industry_limit=args.industry_limit,
                codes=args.industry_codes,
            )
            print(f"已推送 {total} 条行业指标数据。")
        elif args.action == "industry-breadth":
            total = manager.push_industry_breadth(
                lookback_days=args.breadth_window,
                ma_window=args.ma_window,
                indicator_name=args.breadth_indicator,
                save_local=args.save_local,
            )
            print(f"已推送 {total} 条行业宽度数据。")
        else:
            raise ValueError(f"Unsupported backend action: {args.action}")
    finally:
        manager.close()


def handle_auto(args: argparse.Namespace) -> None:
    """High-level orchestrator with two modes: sync data, push data."""
    if args.action == "sync":
        manager = BaostockManager(config_path=args.config)
        try:
            manager.query_stock_basic(refresh=False)
            manager.sync_k_data(
                frequencies=("d",),
                full_update=False,
                lookback_years=args.years,
                resume=True,
            )
        finally:
            manager.close()
        print("增量基础 + 日线同步完成。")
    elif args.action == "push":
        backend = StockMiddlePlatformBackendSync(config_path=args.config)
        try:
            freqs = tuple(args.kline_frequencies or ("d",))
            total_basic = backend.push_stock_basic(batch_size=args.batch_size, limit=args.limit)
            print(f"已推送 {total_basic} 条股票基础数据。")

            total_kline = 0
            for freq in freqs:
                total_kline += backend.push_kline(
                    frequency=freq,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    batch_size=args.batch_size,
                    limit=args.limit,
                )
            print(f"已推送 {total_kline} 条K线数据（频率: {', '.join(freqs)}）。")

            total_metrics = backend.push_industry_metrics(
                lookback_days=args.metrics_window,
                industry_limit=args.industry_limit,
                codes=args.industry_codes,
            )
            print(f"已推送 {total_metrics} 条行业动量/振幅数据。")

            total_breadth = backend.push_industry_breadth(
                lookback_days=args.breadth_window,
                ma_window=args.ma_window,
                indicator_name=args.breadth_indicator,
                save_local=args.save_local,
            )
            print(f"已推送 {total_breadth} 条行业宽度数据。")
        finally:
            backend.close()
    else:
        raise ValueError(f"Unsupported auto action: {args.action}")





def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Data maintenance utilities for AStockDataSync (Baostock/Tushare)."
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
        choices=["basic", "kline", "finance", "limitup-minutes"],
        help=(
            "basic: refresh stock basics; "
            "kline: update K-line collections; "
            "finance: quarterly financials; "
            "limitup-minutes: sync last-week minute bars for limit-up stocks"
        ),
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
        "--resume",
        action="store_true",
        help="When initialization stops midway, continue from the last stored progress.",
    )
    baostock_parser.add_argument(
        "--freq",
        dest="frequencies",
        action="append",
        choices=["d", "w", "m", "5"],
        help="Specify frequencies (repeatable). Defaults to config frequencies.",
    )
    baostock_parser.add_argument(
        "--years",
        type=int,
        default=None,
        help="Custom lookback window for kline/finance operations. Default: config settings.",
    )
    baostock_parser.add_argument(
        "--limitup-days",
        dest="limitup_days",
        type=int,
        default=7,
        help="Lookback days when action=limitup-minutes (default: 7).",
    )
    baostock_parser.add_argument(
        "--pct-threshold",
        dest="pct_threshold",
        type=float,
        default=9.5,
        help="Pct change threshold to mark limit-up when action=limitup-minutes (default: 9.5).",
    )
    baostock_parser.add_argument(
        "--minute-freq",
        dest="minute_frequencies",
        action="append",
        choices=["5"],
        help="Minute frequencies for limit-up sync; repeatable. Default: 5.",
    )
    baostock_parser.set_defaults(handler=handle_baostock)

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

    # Backend commands
    backend_parser = subparsers.add_parser(
        'backend',
        help='Push MongoDB data to stock_middle_platform_backend.',
    )
    backend_parser.add_argument(
        'action',
        choices=['basic', 'kline', 'indicators', 'industry-breadth'],
        help=(
            'basic: push stock_basic; '
            'kline: push historical K-line; '
            'indicators: push industry momentum/width; '
            'industry-breadth: push MA-based industry breadth.'
        ),
    )
    backend_parser.add_argument(
        '--frequency',
        default='d',
        choices=['d', 'w', 'm', '5'],
        help='K线同步频率，仅 action=kline 时有效 (默认: d)。',
    )
    backend_parser.add_argument(
        '--start-date',
        default=None,
        help='K线起始日期 (YYYY-MM-DD 或 YYYYMMDD)。',
    )
    backend_parser.add_argument(
        '--end-date',
        default=None,
        help='K线结束日期 (YYYY-MM-DD 或 YYYYMMDD)。',
    )
    backend_parser.add_argument(
        '--batch-size',
        type=int,
        default=None,
        help='单批推送条数，默认读取配置。',
    )
    backend_parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='限制推送的最大条数，用于调试。',
    )
    backend_parser.add_argument(
        '--metrics-window',
        type=int,
        default=None,
        help='行业指标回溯天数，仅 action=indicators 时生效。',
    )
    backend_parser.add_argument(
        '--industry-limit',
        type=int,
        default=None,
        help='限制推送的行业数量，仅 action=indicators 时生效。',
    )
    backend_parser.add_argument(
        '--industry-codes',
        nargs='+',
        default=None,
        help='仅推送指定申万行业代码 (空格分隔，多选)。',
    )
    backend_parser.add_argument(
        '--breadth-window',
        type=int,
        default=None,
        help='Industry breadth lookback days (default: 30).',
    )
    backend_parser.add_argument(
        '--ma-window',
        type=int,
        default=None,
        help='MA window for breadth calculation (default: 20).',
    )
    backend_parser.add_argument(
        '--breadth-indicator',
        default=None,
        help='Indicator name for breadth payload (default: industry_breadth_ma20).',
    )
    backend_parser.add_argument(
        '--save-local',
        dest="save_local",
        action="store_true",
        help='Persist breadth records locally before pushing (default: enabled).',
    )
    backend_parser.add_argument(
        '--no-save-local',
        dest="save_local",
        action="store_false",
        help='Skip local persistence of breadth records.',
    )
    backend_parser.set_defaults(handler=handle_backend, save_local=None)

    # Auto commands (minimal two-step flow: sync data, push data)
    auto_parser = subparsers.add_parser(
        'auto',
        help='One-click helpers: sync data or push all data.',
    )
    auto_parser.add_argument(
        'action',
        choices=['sync', 'push'],
        help='sync: 增量基础信息+日线; push: 推送基础/K线/指标数据。',
    )
    auto_parser.add_argument(
        '--years',
        type=int,
        default=1,
        help='日线增量回溯年数（仅 action=sync）默认1年。',
    )
    auto_parser.add_argument(
        '--kline-freq',
        dest="kline_frequencies",
        action="append",
        choices=['d', 'w', 'm', '5'],
        help='推送 K 线频率（可重复，action=push）。默认仅 d。',
    )
    auto_parser.add_argument(
        '--start-date',
        default=None,
        help='推送 K 线起始日期，action=push。默认全部。',
    )
    auto_parser.add_argument(
        '--end-date',
        default=None,
        help='推送 K 线结束日期，action=push。',
    )
    auto_parser.add_argument(
        '--batch-size',
        type=int,
        default=None,
        help='推送批大小，action=push。',
    )
    auto_parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='推送数量上限（调试用），action=push。',
    )
    auto_parser.add_argument(
        '--metrics-window',
        type=int,
        default=None,
        help='行业指标回溯天数，action=push。',
    )
    auto_parser.add_argument(
        '--industry-limit',
        type=int,
        default=None,
        help='行业数量上限，action=push。',
    )
    auto_parser.add_argument(
        '--industry-codes',
        nargs='+',
        default=None,
        help='指定推送的申万行业代码，action=push。',
    )
    auto_parser.add_argument(
        '--breadth-window',
        type=int,
        default=None,
        help='行业宽度回溯天数，action=push。',
    )
    auto_parser.add_argument(
        '--ma-window',
        type=int,
        default=None,
        help='行业宽度 MA 窗口，action=push。',
    )
    auto_parser.add_argument(
        '--breadth-indicator',
        default=None,
        help='行业宽度指标名，action=push。',
    )
    auto_parser.add_argument(
        '--save-local',
        dest="save_local",
        action="store_true",
        help='推送前本地落库行业宽度，action=push。',
    )
    auto_parser.add_argument(
        '--no-save-local',
        dest="save_local",
        action="store_false",
        help='跳过本地落库行业宽度，action=push。',
    )
    auto_parser.set_defaults(
        handler=handle_auto,
        kline_frequencies=None,
        save_local=None,
    )

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
