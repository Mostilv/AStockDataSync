import argparse
import sys
import os
import time
from datetime import datetime
from typing import Callable, List, Optional

os.environ["PYTHONIOENCODING"] = "utf-8"

from .data.manager_akshare import AkshareManager
from .data.manager_backend import StockMiddlePlatformBackendSync
from .indicators.industry_breadth import IndustryBreadthCalculator


def handle_akshare(args: argparse.Namespace) -> None:
    config_path = args.config
    with AkshareManager(config_path=config_path) as manager:
        if args.command == "basic":
            print("初始化股票列表 (stock_zh_a_spot_em)")
            manager.query_stock_basic()
            print("基础信息更新完毕。")
            return

        elif args.command == "kline":
            if not args.frequencies:
                print("请指定至少一个频率，例如 -f d w")
                return
            manager.sync_k_data(
                frequencies=args.frequencies,
                full_update=args.full_update,
                lookback_years=args.lookback_years,
                resume=not args.no_resume,
            )
            print("历史 K 线同步完毕。")
            return

        elif args.command == "spot":
            print(f"抓取一次实时快照 (循环模式: {args.loop})")
            if args.loop:
                manager.run_loop(interval_minutes=args.interval)
            else:
                manager.sync_once()
            print("快照获取/更新完毕。")
            return

        elif args.command == "validation":
            print("执行盘后历史数据校验...")
            manager.run_validation(frequencies=args.frequencies or ["d", "w", "m", "5"])
            print("盘后校验完毕。")
            return

        print(f"未知的 akshare 子命令: {args.command}")


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
        manager = AkshareManager(config_path=args.config)
        try:
            manager.query_stock_basic()
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
        description="Data maintenance utilities for AStockDataSync (Akshare)."
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to YAML config file (default: config.yaml)",
    )

    subparsers = parser.add_subparsers(dest="source", required=True)

    # Akshare commands
    akshare_parser = subparsers.add_parser("akshare", help="Tasks for Akshare")
    akshare_parser.add_argument(
        "command",
        choices=["basic", "kline", "spot", "validation"],
        help="Command to run.",
    )
    akshare_parser.add_argument(
        "-f", "--frequencies", nargs="+", help="Frequencies to sync (d, w, m, 5)."
    )
    akshare_parser.add_argument(
        "--full-update", action="store_true", help="Force a full history fetching instead of resume."
    )
    akshare_parser.add_argument(
        "--no-resume", action="store_true", help="Disable resume capability when fetching history."
    )
    akshare_parser.add_argument(
        "--lookback-years", type=int, default=3, help="Years of history to fetch."
    )
    akshare_parser.add_argument(
        "--loop", action="store_true", help="Running spot fetch in a loop."
    )
    akshare_parser.add_argument(
        "--interval", type=int, default=30, help="Loop interval in minutes."
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
