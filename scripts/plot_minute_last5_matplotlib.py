import argparse
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional

import matplotlib
from matplotlib import font_manager
import mplcursors
import mplfinance as mpf
import pandas as pd
from pymongo import MongoClient

_CH_FONT_CANDIDATES = [
    "Microsoft YaHei",
    "SimHei",
    "PingFang SC",
    "WenQuanYi Micro Hei",
    "STSong",
    "Noto Sans CJK SC",
]
_available_fonts = {f.name for f in font_manager.fontManager.ttflist}
for _font in _CH_FONT_CANDIDATES:
    if _font in _available_fonts:
        matplotlib.rcParams["font.sans-serif"] = [_font, "DejaVu Sans"]
        break
else:
    matplotlib.rcParams.setdefault("font.sans-serif", ["DejaVu Sans"])
matplotlib.rcParams["axes.unicode_minus"] = False

# 确保仓库根目录在 sys.path 中
BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from src.utils.config_loader import load_config


def pick_code(col, start_date: str, preferred: Optional[str] = None) -> Optional[str]:
    if preferred:
        exists = col.find_one({"code": preferred, "date": {"$gte": start_date}})
        return preferred if exists else None
    codes = col.distinct("code", {"date": {"$gte": start_date}})
    if not codes:
        return None
    return random.choice(codes)


def keep_last_sessions(df: pd.DataFrame, sessions: int = 5) -> pd.DataFrame:
    """
    保留最近 sessions 个交易日的数据（按索引的日期部分判断）。
    """
    if df.empty or sessions <= 0:
        return df
    normalized = df.index.normalize()
    unique_days = normalized.unique()
    if len(unique_days) <= sessions:
        return df
    cutoff_day = unique_days[-sessions]
    mask = normalized >= cutoff_day
    return df.loc[mask]


def load_minutes(col, code: str, start_date: str, last_sessions: int = 5) -> pd.DataFrame:
    start_digits = start_date.replace("-", "")
    date_filters = [{"date": {"$gte": start_date}}]
    start_dt_obj = pd.to_datetime(start_date, errors="coerce")
    if pd.notna(start_dt_obj):
        date_filters.append({"date": {"$gte": start_dt_obj.to_pydatetime()}})
    if start_digits:
        date_filters.append({"date": {"$gte": start_digits}})
        if start_digits.isdigit():
            date_filters.append({"date": {"$gte": int(start_digits)}})

    query = {"code": code}
    if len(date_filters) == 1:
        query.update(date_filters[0])
    else:
        query["$or"] = date_filters

    cursor = (
        col.find(query, {"_id": 0})
        .sort([("date", 1), ("time", 1)])
    )

    def _normalize_date(date_val) -> Optional[str]:
        if pd.isna(date_val):
            return None
        s = str(date_val).strip()
        if not s:
            return None
        s = s.replace("/", "-")
        for sep in (" ", "T"):
            if sep in s:
                s = s.split(sep)[0]
        digits = s.replace("-", "")
        if digits.isdigit() and len(digits) == 8:
            s = f"{digits[:4]}-{digits[4:6]}-{digits[6:]}"
        dt = pd.to_datetime(s, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.strftime("%Y-%m-%d")

    def _normalize_time(time_val) -> Optional[str]:
        if pd.isna(time_val):
            return "00:00:00"
        s = str(time_val).strip()
        if not s:
            return None
        try:
            if "e" in s.lower() or "." in s:
                s = str(int(float(s)))
        except ValueError:
            pass
        digits_only = "".join(ch for ch in s if ch.isdigit())
        if len(digits_only) >= 14:  # e.g. 20251208150000000
            tpart = digits_only[8:14]
            return f"{tpart[:2]}:{tpart[2:4]}:{tpart[4:6]}"
        if digits_only.isdigit():
            if len(digits_only) <= 4:  # HHMM without seconds
                digits_only = digits_only.zfill(4)
                return f"{digits_only[:2]}:{digits_only[2:4]}:00"
            digits_only = digits_only.zfill(6)
            return f"{digits_only[:2]}:{digits_only[2:4]}:{digits_only[4:6]}"
        if ":" in s:
            parts = s.split(":")
            parts = (parts + ["00", "00", "00"])[:3]
            parts = [parts[0].zfill(2), parts[1].zfill(2), parts[2].zfill(2)]
            return ":".join(parts)
        return None

    def _normalize_timestamp(date_val, time_val) -> Optional[str]:
        date_str = _normalize_date(date_val)
        time_str = _normalize_time(time_val)
        if not date_str or not time_str:
            return None
        return f"{date_str} {time_str}"

    df = pd.DataFrame(list(cursor))
    if df.empty:
        return df

    timestamps = df.apply(lambda r: _normalize_timestamp(r.get("date"), r.get("time")), axis=1)
    df["timestamp"] = pd.to_datetime(timestamps, errors="coerce")
    df = df.dropna(subset=["timestamp"])
    df = df.set_index("timestamp").sort_index()
    return keep_last_sessions(df, last_sessions)


def plot_matplotlib(df: pd.DataFrame, code: str) -> None:
    numeric_cols = ["open", "high", "low", "close", "volume"]
    df_plot = df[numeric_cols].apply(pd.to_numeric, errors="coerce").dropna()
    # 去重以避免同一时间戳的重复点挤压坐标
    df_plot = df_plot[~df_plot.index.duplicated(keep="first")]
    df_plot = df_plot.sort_index()
    df_plot.index = pd.DatetimeIndex(df_plot.index)
    df_plot.index.name = "Date"
    if df_plot.empty:
        print("无有效数据可绘制")
        return
    min_ts, max_ts = df_plot.index.min(), df_plot.index.max()
    days = df_plot.index.normalize()
    grouped = df_plot.groupby(days)
    print(f"共 {len(df_plot)} 条，交易日 {grouped.ngroups} 个，时间范围 {min_ts} ~ {max_ts}")
    for day, sub in grouped:
        start_t = sub.index.min().strftime("%H:%M:%S")
        end_t = sub.index.max().strftime("%H:%M:%S")
        print(f"  {day.date()}: {len(sub)} bars {start_t} ~ {end_t}")

    fig, axes = mpf.plot(
        df_plot,
        type="candle",
        volume=True,
        title=f"{code} 最近5天分钟级别K线",
        style="yahoo",
        returnfig=True,
        figsize=(12, 6),
        datetime_format="%m-%d %H:%M",
        show_nontrading=False,
        tight_layout=True,
    )

    price_ax = axes[0]
    vol_ax = axes[2] if len(axes) > 2 else axes[1]

    cursors = mplcursors.cursor(price_ax.collections, hover=True)

    @cursors.connect("add")
    def _on_add(sel):
        x, _ = sel.target
        try:
            idx = int(round(x))
        except Exception:
            return
        if 0 <= idx < len(df_plot):
            ts = df_plot.index[idx]
            row = df_plot.iloc[idx]
            sel.annotation.set(
                text=(
                    f"{ts:%Y-%m-%d %H:%M}\n"
                    f"开:{row['open']:.2f} 高{row['high']:.2f}\n"
                    f"低{row['low']:.2f} 收{row['close']:.2f}\n"
                    f"量{row['volume']:.0f}"
                )
            )

    if len(df_plot) and vol_ax:
        vol_hover_points = vol_ax.scatter(
            df_plot.index,
            df_plot["volume"],
            s=10,
            alpha=0.0,
            picker=True,
            zorder=3,
            label="_nolegend_",
        )
        vol_cursor = mplcursors.cursor(vol_hover_points, hover=True)

        @vol_cursor.connect("add")
        def _on_add_vol(sel):
            try:
                idx = int(sel.index)
            except Exception:
                return
            if 0 <= idx < len(df_plot):
                ts = df_plot.index[idx]
                vol = df_plot["volume"].iloc[idx]
                sel.annotation.set(text=f"{ts:%Y-%m-%d %H:%M}\n量{vol:.0f}")

    mpf.show()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Plot a random stock/index minute bars for the last 5 days (matplotlib interactive)."
    )
    parser.add_argument("--config", default="config.yaml", help="Path to YAML config.")
    parser.add_argument("--code", default=None, help="Optional code to force (e.g., sh.000001).")
    args = parser.parse_args()

    cfg_path = Path(args.config)
    if not cfg_path.is_absolute():
        cfg_path = (BASE_DIR / cfg_path).resolve()
    if not cfg_path.exists():
        raise FileNotFoundError(f"配置文件未找到 {cfg_path}")

    cfg: Dict = load_config(str(cfg_path))
    baostock_cfg = cfg.get("baostock", {})
    mongo_uri = cfg.get("mongodb", {}).get("uri", "mongodb://localhost:27017/")

    minute_col_name = baostock_cfg.get("minute_5", "minute_5_adjusted")
    db_name = baostock_cfg.get("db", "baostock")

    client = MongoClient(mongo_uri)
    col = client[db_name][minute_col_name]

    start_dt = datetime.now() - timedelta(days=10)
    start_date = start_dt.strftime("%Y-%m-%d")

    code = pick_code(col, start_date, preferred=args.code)
    if not code:
        print("未找到最近天的分钟数据，请确认库中有记录。")
        return

    df = load_minutes(col, code, start_date)
    if df.empty:
        print(f"{code} 最近天无分钟数据。")
        return

    print(f"选中代码: {code}")
    print(f"数据条数: {len(df)}，时间范围: {df.index.min()} ~ {df.index.max()}")
    print("即将弹出 matplotlib 窗口，鼠标悬浮可查看精确数值。")
    plot_matplotlib(df, code)


if __name__ == "__main__":
    main()
