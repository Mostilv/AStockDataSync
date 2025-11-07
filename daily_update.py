import argparse
from typing import List, Optional

from mosquant.data.manager_akshare import AkshareRealtimeManager
from mosquant.data.manager_baostock import BaostockManager


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Daily maintenance helper: Baostock history + Akshare realtime补齐。"
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="YAML 配置路径（默认: config.yaml）。",
    )
    parser.add_argument(
        "--frequencies",
        nargs="+",
        default=["d"],
        help="需要同步的 baostock 周期，默认仅日线(d)。",
    )
    parser.add_argument(
        "--years",
        type=int,
        default=None,
        help="当仓库为空时的回溯年限，默认读取配置中的 history_years。",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="强制从 START_DATE 全量回补。",
    )
    parser.add_argument(
        "--refresh-basic",
        action="store_true",
        help="同步前刷新股票基本信息。",
    )
    parser.add_argument(
        "--skip-akshare",
        action="store_true",
        help="仅跑 baostock，不触发 Akshare 快照。",
    )
    parser.add_argument(
        "--akshare-loop",
        action="store_true",
        help="启动 Akshare 实时循环模式（否则只拉一次并刷新当日 bar）。",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=None,
        help="Akshare 循环次数（默认无限）。",
    )
    parser.add_argument(
        "--ignore-hours",
        action="store_true",
        help="允许在盘外执行 Akshare 拉取。",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="仅打印计划，不实际请求外部数据源。",
    )
    return parser.parse_args()


def run_baostock(
    config_path: str,
    frequencies: List[str],
    years: Optional[int],
    full_update: bool,
    refresh_basic: bool,
) -> None:
    with BaostockManager(config_path=config_path) as manager:
        if refresh_basic:
            manager.query_stock_basic(refresh=False)
        manager.sync_k_data(
            frequencies=frequencies,
            lookback_years=years,
            full_update=full_update,
        )


def run_akshare(
    config_path: str,
    loop_mode: bool,
    iterations: Optional[int],
    ignore_hours: bool,
) -> None:
    with AkshareRealtimeManager(config_path=config_path) as manager:
        if loop_mode:
            manager.run_loop(iterations=iterations, ignore_trading_window=ignore_hours)
        else:
            manager.sync_once(ignore_trading_window=ignore_hours, force_flush=True)


def main() -> None:
    args = parse_args()

    if args.dry_run:
        print(
            f"[Dry Run] 将使用 {args.config}，Baostock 周期={args.frequencies}，"
            f"full_update={args.full}, years={args.years}; "
            f"Akshare loop={args.akshare_loop}, iterations={args.iterations}, skip_akshare={args.skip_akshare}"
        )
        return

    run_baostock(
        config_path=args.config,
        frequencies=args.frequencies,
        years=args.years,
        full_update=args.full,
        refresh_basic=args.refresh_basic,
    )

    if not args.skip_akshare:
        run_akshare(
            config_path=args.config,
            loop_mode=args.akshare_loop,
            iterations=args.iterations,
            ignore_hours=args.ignore_hours,
        )


if __name__ == "__main__":
    main()
