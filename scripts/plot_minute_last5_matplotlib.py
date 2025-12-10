import argparse
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional

import mplcursors
import mplfinance as mpf
import pandas as pd
from pymongo import MongoClient

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


def load_minutes(col, code: str, start_date: str) -> pd.DataFrame:
    cursor = (
        col.find({"code": code, "date": {"$gte": start_date}}, {"_id": 0})
        .sort([("date", 1), ("time", 1)])
    )

    def _normalize_timestamp(date_str, time_val) -> Optional[str]:
        """
        将 Baostock 浮点/科学计数法时间或 HH:MM:SS 统一格式化为 ISO 字符串。
        例如：
        - 20251208150000000 -> 2025-12-08 15:00:00
        - 150000            -> 2025-12-08 15:00:00
        - HH:MM:SS          -> 直接拼接
        """
        if pd.isna(time_val):
            return f"{date_str} 00:00:00"
        s = str(time_val).strip()
        # 去掉科学计数/小数点
        try:
            if "e" in s.lower() or "." in s:
                s = str(int(float(s)))
        except ValueError:
            pass

        if len(s) >= 14:  # 20251208150000000
            date_part = s[:8]
            tpart = s[8:14]
            return f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:]} {tpart[:2]}:{tpart[2:4]}:{tpart[4:6]}"
        if len(s) == 6:  # 150000
            return f"{date_str} {s[:2]}:{s[2:4]}:{s[4:6]}"
        if ":" in s:
            return f"{date_str} {s}"
        return None

    df = pd.DataFrame(list(cursor))
    if df.empty:
        return df
    df["date"] = df["date"].astype(str)

    timestamps = df.apply(lambda r: _normalize_timestamp(r["date"], r.get("time")), axis=1)
    df["timestamp"] = pd.to_datetime(timestamps, errors="coerce")
    df = df.dropna(subset=["timestamp"])
    df = df.set_index("timestamp")
    return df


def plot_matplotlib(df: pd.DataFrame, code: str) -> None:
    df_plot = df[["open", "high", "low", "close", "volume"]].copy()
    fig, axes = mpf.plot(
        df_plot,
        type="candle",
        volume=True,
        title=f"{code} 最近天分钟级别K线",
        style="yahoo",
        returnfig=True,
        figsize=(12, 6),
        datetime_format="%m-%d %H:%M",
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

    if vol_ax.patches:
        vol_cursor = mplcursors.cursor(vol_ax.patches, hover=True)

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
