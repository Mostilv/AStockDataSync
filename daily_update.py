import argparse
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import baostock as bs

from src.data.manager_akshare import AkshareRealtimeManager
from src.data.manager_baostock import BaostockManager
from src.indicators.registry import run_indicator_suite
from src.utils.config_loader import load_config

DATE_FMT = "%Y-%m-%d"
TRADE_CAL_LOOKBACK_DAYS = 120
TRADE_CAL_LOOKAHEAD_DAYS = 10
DEFAULT_CONFIG_PATH = "config.yaml"


class TradeCalendarHelper:
    """Thin wrapper over baostock trade calendar to decide weekly/monthly windows."""

    def __init__(
        self,
        lookback_days: int = TRADE_CAL_LOOKBACK_DAYS,
        lookahead_days: int = TRADE_CAL_LOOKAHEAD_DAYS,
    ) -> None:
        self.lookback_days = lookback_days
        self.lookahead_days = lookahead_days
        self._open_dates: List[date] = []

    def latest_trading_day(self, reference: Optional[date] = None) -> Optional[date]:
        end_date = reference or datetime.now().date()
        start_date = end_date - timedelta(days=self.lookback_days)
        return self._last_open_date(start_date, end_date)

    def next_trading_day(self, trade_date: date) -> Optional[date]:
        start = trade_date + timedelta(days=1)
        end = start + timedelta(days=self.lookahead_days)
        return self._first_open_date(start, end)

    def should_update_weekly(self, reference: Optional[date] = None) -> bool:
        latest = self.latest_trading_day(reference)
        if not latest:
            return True
        next_open = self.next_trading_day(latest)
        if not next_open:
            return True
        return next_open.isocalendar()[1] != latest.isocalendar()[1]

    def should_update_monthly(self, reference: Optional[date] = None) -> bool:
        latest = self.latest_trading_day(reference)
        if not latest:
            return True
        next_open = self.next_trading_day(latest)
        if not next_open:
            return True
        return next_open.month != latest.month

    def expected_weekly_close(self, reference: Optional[date] = None) -> Optional[date]:
        open_dates = self._collect_open_dates(reference)
        if not open_dates:
            return None
        week_keys = sorted({(d.isocalendar()[0], d.isocalendar()[1]) for d in open_dates})
        if len(week_keys) >= 2:
            target = week_keys[-2]
            dates = [d for d in open_dates if (d.isocalendar()[0], d.isocalendar()[1]) == target]
            return max(dates) if dates else open_dates[-1]
        return open_dates[-1]

    def expected_monthly_close(self, reference: Optional[date] = None) -> Optional[date]:
        open_dates = self._collect_open_dates(reference)
        if not open_dates:
            return None
        month_keys = sorted({(d.year, d.month) for d in open_dates})
        if len(month_keys) >= 2:
            target = month_keys[-2]
            dates = [d for d in open_dates if (d.year, d.month) == target]
            return max(dates) if dates else open_dates[-1]
        return open_dates[-1]

    def _last_open_date(self, start: date, end: date) -> Optional[date]:
        last_open: Optional[date] = None
        for day_value, is_open in self._iter_trade_dates(start, end):
            if is_open:
                last_open = day_value
        return last_open

    def _first_open_date(self, start: date, end: date) -> Optional[date]:
        for day_value, is_open in self._iter_trade_dates(start, end):
            if is_open:
                return day_value
        return None

    def _iter_trade_dates(self, start: date, end: date) -> Iterable[Tuple[date, bool]]:
        rs = bs.query_trade_dates(start.strftime(DATE_FMT), end.strftime(DATE_FMT))
        if rs.error_code != "0":
            print(f"Failed to query baostock trade dates: {rs.error_msg}")
            return []
        fields = [f.lower() for f in (rs.fields or [])]
        date_idx = next((i for i, f in enumerate(fields) if "date" in f), 0)
        open_idx = next(
            (i for i, f in enumerate(fields) if "isopen" in f or "istrade" in f or "is_trading" in f),
            1 if len(fields) > 1 else 0,
        )
        while rs.next():
            row = rs.get_row_data()
            if not row:
                continue
            try:
                raw_date = row[date_idx]
                is_open_val = row[open_idx] if open_idx < len(row) else (row[1] if len(row) > 1 else "0")
                day_value = datetime.strptime(raw_date, DATE_FMT).date()
                yield day_value, str(is_open_val).strip() == "1"
            except Exception:
                continue

    def _collect_open_dates(self, reference: Optional[date] = None) -> List[date]:
        if self._open_dates:
            return self._open_dates
        end_date = reference or datetime.now().date()
        start_date = end_date - timedelta(days=self.lookback_days)
        open_dates: List[date] = []
        for day_value, is_open in self._iter_trade_dates(start_date, end_date):
            if is_open:
                open_dates.append(day_value)
        open_dates.sort()
        self._open_dates = open_dates
        return open_dates


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Config-driven daily maintenance entrypoint.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned tasks without hitting external data sources.",
    )
    return parser.parse_args()


def load_daily_job_config(config_path: str) -> Tuple[Dict, Dict]:
    config = load_config(config_path)
    workflow_cfg = config.get("workflow", {}) or {}
    daily_cfg = workflow_cfg.get("daily_update") or workflow_cfg.get("daily") or {}
    return config, daily_cfg


def resolve_frequencies(
    requested: Sequence[str],
    schedule_cfg: Dict,
    planner: TradeCalendarHelper,
    backfill_flags: Optional[Dict[str, bool]] = None,
) -> Tuple[str, ...]:
    requested = tuple(requested or ())
    if not requested:
        return tuple()

    enable_weekly = bool(schedule_cfg.get("weekly", True))
    enable_monthly = bool(schedule_cfg.get("monthly", True))
    resolved: List[str] = []
    for freq in requested:
        if backfill_flags and backfill_flags.get(freq):
            resolved.append(freq)
            continue
        if freq == "w":
            if enable_weekly and planner.should_update_weekly():
                resolved.append(freq)
            else:
                print("Skip weekly sync today (not the end of a trading week).")
        elif freq == "m":
            if enable_monthly and planner.should_update_monthly():
                resolved.append(freq)
            else:
                print("Skip monthly sync today (not the end of a trading month).")
        else:
            resolved.append(freq)
    return tuple(resolved)


def compute_backfill_flags(manager: BaostockManager, planner: TradeCalendarHelper) -> Dict[str, bool]:
    flags: Dict[str, bool] = {}
    expected_week = planner.expected_weekly_close()
    expected_month = planner.expected_monthly_close()
    flags["w"] = manager.needs_backfill("w", expected_week.strftime(DATE_FMT) if expected_week else None)
    flags["m"] = manager.needs_backfill("m", expected_month.strftime(DATE_FMT) if expected_month else None)
    return flags


def run_baostock_job(config_path: str, config: Dict, daily_cfg: Dict, dry_run: bool = False) -> None:
    bs_job_cfg = daily_cfg.get("baostock", {}) or {}
    refresh_basic = bool(daily_cfg.get("refresh_basic", True))
    full_update = bool(bs_job_cfg.get("full_update", False))
    resume = bool(bs_job_cfg.get("resume", True))
    lookback_years = bs_job_cfg.get("lookback_years", config.get("baostock", {}).get("history_years"))
    lookback_years = int(lookback_years) if lookback_years is not None else None
    schedule_cfg = bs_job_cfg.get("schedule", {}) or {}
    base_frequencies = bs_job_cfg.get("frequencies") or config.get("baostock", {}).get(
        "frequencies", ["d", "w", "m", "5"]
    )
    tagging_cfg = daily_cfg.get("tagging", {}) or {}
    include_industry = bool(tagging_cfg.get("industry", True))
    include_concept = bool(tagging_cfg.get("concept", True))

    with BaostockManager(config_path=config_path) as manager:
        planner = TradeCalendarHelper()
        backfill_flags = compute_backfill_flags(manager, planner)
        frequencies = resolve_frequencies(base_frequencies, schedule_cfg, planner, backfill_flags=backfill_flags)

        if dry_run:
            print(
                f"[Dry Run] baostock sync -> refresh_basic={refresh_basic}, "
                f"frequencies={frequencies}, full_update={full_update}, "
                f"resume={resume}, lookback_years={lookback_years}, "
                f"industry_tag={include_industry}, concept_tag={include_concept}"
            )
            return

        if refresh_basic:
            manager.query_stock_basic(refresh=False)
        if include_industry or include_concept:
            manager.refresh_industry_and_concepts(
                include_industry=include_industry,
                include_concept=include_concept,
            )

        if not frequencies:
            print("No baostock K-line frequency scheduled today; skipping K-line sync.")
            return

        manager.sync_k_data(
            frequencies=frequencies,
            full_update=full_update,
            lookback_years=lookback_years,
            resume=resume,
        )


def run_akshare_job(config_path: str, daily_cfg: Dict, dry_run: bool = False) -> None:
    ak_cfg = daily_cfg.get("akshare", {}) or {}
    if not ak_cfg.get("enabled", False):
        return

    loop_mode = bool(ak_cfg.get("loop_mode", False))
    iterations = ak_cfg.get("iterations")
    ignore_hours = bool(ak_cfg.get("ignore_hours", False))

    if dry_run:
        print(
            f"[Dry Run] akshare realtime -> loop_mode={loop_mode}, iterations={iterations}, "
            f"ignore_trading_window={ignore_hours}"
        )
        return

    with AkshareRealtimeManager(config_path=config_path) as manager:
        if loop_mode:
            manager.run_loop(iterations=iterations, ignore_trading_window=ignore_hours)
        else:
            manager.sync_once(ignore_trading_window=ignore_hours, force_flush=True)


def run_indicator_jobs(config_path: str, config: Dict, daily_cfg: Dict, dry_run: bool = False) -> None:
    indicator_cfg = daily_cfg.get("indicators", {}) or {}
    if not indicator_cfg.get("enabled", True):
        return

    jobs = indicator_cfg.get("jobs") or []
    run_indicator_suite(config_path=config_path, jobs=jobs, dry_run=dry_run)


def main() -> None:
    args = parse_args()
    config_path = DEFAULT_CONFIG_PATH
    config, daily_cfg = load_daily_job_config(config_path)

    run_baostock_job(config_path, config, daily_cfg, dry_run=args.dry_run)
    run_akshare_job(config_path, daily_cfg, dry_run=args.dry_run)
    run_indicator_jobs(config_path, config, daily_cfg, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
