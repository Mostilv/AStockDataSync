import argparse
from datetime import datetime
from typing import List, Optional

from mosquant.data.manager_akshare import AkshareRealtimeManager
from mosquant.data.manager_baostock import BaostockManager


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Daily maintenance helper: Baostock history + Akshare realtime 补齐"
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="YAML 配置路径（默认 config.yaml）。",
    )
    parser.add_argument(
        "--frequencies",
        nargs="+",
        default=["d", "w", "m", "15", "60"],
        help="需要同步的 baostock 周期，默认覆盖日/周/月/15m/60m。",
    )
    parser.add_argument(
        "--years",
        type=int,
        default=None,
        help="当仓库为空时的 K 线回溯年限，默认读取配置中的 history_years。",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="强制从配置的起始日期全量回补，无视 last_* 标记。",
    )
    parser.add_argument(
        "--refresh-basic",
        action="store_true",
        help="同步前刷新股票基本信息。",
    )
    parser.add_argument(
        "--skip-finance",
        action="store_true",
        help="跳过季频财务数据同步。",
    )
    parser.add_argument(
        "--finance-years",
        type=int,
        default=None,
        help="季频财务数据的回溯年限，默认读取配置 finance_history_years。",
    )
    parser.add_argument(
        "--skip-integrity-check",
        action="store_true",
        help="禁用周末自动完整性校验。",
    )
    parser.add_argument(
        "--force-integrity",
        action="store_true",
        help="无论日期均执行一次完整性校验。",
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
    include_finance: bool,
    finance_years: Optional[int],
    run_integrity: bool,
) -> None:
    with BaostockManager(config_path=config_path) as manager:
        if refresh_basic:
            manager.query_stock_basic(refresh=False)
        manager.sync_k_data(
            frequencies=frequencies,
            lookback_years=years,
            full_update=full_update,
        )
        if include_finance:
            manager.sync_finance_data(
                full_update=full_update,
                years=finance_years,
            )
        if run_integrity:
            manager.run_integrity_check()


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
    today = datetime.now()
    is_weekend = today.weekday() >= 5
    should_integrity = args.force_integrity or (is_weekend and not args.skip_integrity_check)

    if args.dry_run:
        print(
            f"[Dry Run] config={args.config}, freq={args.frequencies}, full={args.full}, "
            f"kline_years={args.years}, finance={not args.skip_finance} "
            f"(years={args.finance_years}), integrity={should_integrity}; "
            f"Akshare loop={args.akshare_loop}, iterations={args.iterations}, "
            f"skip_akshare={args.skip_akshare}"
        )
        return

    run_baostock(
        config_path=args.config,
        frequencies=args.frequencies,
        years=args.years,
        full_update=args.full,
        refresh_basic=args.refresh_basic,
        include_finance=not args.skip_finance,
        finance_years=args.finance_years,
        run_integrity=should_integrity,
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
