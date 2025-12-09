import logging
from datetime import datetime, time, timedelta
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import akshare as ak
import pandas as pd
from pymongo import DESCENDING, MongoClient

from ..utils.config_loader import load_config


logger = logging.getLogger(__name__)


class IndustryBreadthCalculator:
    """Compute industry breadth = pct of members closing above MA window."""

    def __init__(
        self,
        config_path: str = "config.yaml",
        *,
        indicator: Optional[str] = None,
        timeframe: str = "1d",
        lookback_days: Optional[int] = None,
        ma_window: Optional[int] = None,
        collection_name: Optional[str] = None,
        save_local: bool = True,
    ) -> None:
        self.config_path = config_path
        self.config = load_config(config_path)
        mongo_cfg = self.config["mongodb"]
        baostock_cfg = self.config["baostock"]
        backend_cfg = self.config.get("stock_middle_platform_backend", {}) or {}
        breadth_cfg = backend_cfg.get("industry_breadth", {}) or {}

        self.indicator = (indicator or breadth_cfg.get("indicator") or "industry_breadth_ma20").strip()
        self.timeframe = (timeframe or breadth_cfg.get("timeframe") or "1d").strip().lower()
        self.lookback_days = int(lookback_days or breadth_cfg.get("lookback_days") or 30)
        self.ma_window = int(ma_window or breadth_cfg.get("ma_window") or 20)
        self.collection_name = (collection_name or breadth_cfg.get("collection") or "indicator_data").strip()
        self.save_local = bool(save_local if save_local is not None else breadth_cfg.get("save_local", True))

        self.client = MongoClient(mongo_cfg["uri"])
        self.db = self.client[baostock_cfg["db"]]
        self.daily_col = self.db[baostock_cfg["daily"]]
        self.basic_col = self.db[baostock_cfg["basic"]]
        self.indicator_col = self.db[self.collection_name]

    def close(self) -> None:
        self.client.close()

    def collect(self) -> List[Dict]:
        industries = self._load_industries()
        if not industries:
            return []

        industry_members = self._build_member_map(industries)
        if not industry_members:
            return []

        code_to_industry: Dict[str, str] = {}
        for code, items in industry_members.items():
            for industry_code in items:
                code_to_industry[code] = industry_code

        start_dt, end_dt = self._resolve_window()
        start_str = start_dt.strftime("%Y-%m-%d")
        end_str = end_dt.strftime("%Y-%m-%d")

        cursor = self.daily_col.find(
            {
                "code": {"$in": list(code_to_industry.keys())},
                "date": {"$gte": start_str, "$lte": end_str},
            },
            {"code": 1, "date": 1, "close": 1},
        )
        rows = list(cursor)
        if not rows:
            logger.warning("No daily rows found for industry breadth window %s ~ %s", start_str, end_str)
            return []

        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["code", "date", "close"])
        if df.empty:
            logger.warning("Daily dataframe is empty after cleaning for industry breadth calc.")
            return []

        df["code"] = df["code"].astype(str)
        df = df.sort_values(["code", "date"])
        df["ma"] = (
            df.groupby("code")["close"]
            .transform(lambda series: series.rolling(self.ma_window, min_periods=self.ma_window).mean())
        )
        df = df.dropna(subset=["ma"])
        if df.empty:
            logger.warning("Insufficient history to compute %s-day MA for breadth.", self.ma_window)
            return []

        df["above"] = df["close"] > df["ma"]
        df["industry"] = df["code"].map(code_to_industry)
        df = df.dropna(subset=["industry"])
        if df.empty:
            logger.warning("No industry mapping matched for breadth calculation.")
            return []

        grouped = (
            df.groupby(["industry", "date"])
            .agg(total=("above", "size"), above=("above", "sum"))
            .reset_index()
        )
        grouped["breadth"] = (grouped["above"] / grouped["total"]) * 100

        name_map = {item["code"]: item["name"] for item in industries}
        records: List[Dict] = []
        for _, row in grouped.iterrows():
            dt_value = row["date"]
            if pd.isna(dt_value):
                continue
            ts = datetime.combine(pd.to_datetime(dt_value).date(), time())
            industry_code = str(row["industry"])
            record = {
                "indicator": self.indicator,
                "symbol": f"INDUSTRY:{industry_code}",
                "timeframe": self.timeframe,
                "timestamp": ts.isoformat(),
                "value": round(float(row["breadth"]), 4),
                "values": {
                    "breadth_pct": round(float(row["breadth"]), 4),
                    "above_ma": int(row["above"]),
                    "total": int(row["total"]),
                    "ma_window": self.ma_window,
                },
                "payload": {
                    "industry_code": industry_code,
                    "industry_name": name_map.get(industry_code, industry_code),
                },
                "tags": ["industry", "breadth", f"ma{self.ma_window}"],
            }
            records.append(record)

        if self.save_local and records:
            self._persist(records)
        return records

    def _resolve_window(self) -> Tuple[datetime, datetime]:
        latest = self.daily_col.find_one({}, {"date": 1}, sort=[("date", DESCENDING)])
        if not latest or not latest.get("date"):
            raise RuntimeError("Cannot resolve end date from daily collection.")
        end_dt = datetime.strptime(latest["date"], "%Y-%m-%d")
        start_dt = end_dt - timedelta(days=self.lookback_days + self.ma_window + 5)
        return start_dt, end_dt

    def _load_industries(self) -> List[Dict[str, str]]:
        try:
            df = ak.sw_index_first_info()
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to load Shenwan level-1 industries: %s", exc)
            return []
        if df is None or df.empty:
            return []
        df = df[["行业代码", "行业名称"]].dropna()
        return [
            {
                "code": str(row["行业代码"]).replace(".SI", "").strip(),
                "name": str(row["行业名称"]).strip(),
            }
            for _, row in df.iterrows()
        ]

    def _build_member_map(self, industries: Iterable[Dict[str, str]]) -> Dict[str, List[str]]:
        mapping: Dict[str, List[str]] = {}
        for info in industries:
            code = info["code"]
            try:
                members = ak.index_component_sw(code)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to load members for %s: %s", code, exc)
                continue
            if members is None or members.empty:
                continue
            for _, row in members.iterrows():
                raw = str(row.get("证券代码") or "").strip()
                if not raw:
                    continue
                normalized = self._normalize_stock_code(raw)
                if not normalized:
                    continue
                mapping.setdefault(normalized, []).append(code)
        return mapping

    def _persist(self, records: Sequence[Dict]) -> None:
        for doc in records:
            try:
                self.indicator_col.update_one(
                    {
                        "indicator": doc["indicator"],
                        "symbol": doc["symbol"],
                        "timeframe": doc["timeframe"],
                        "timestamp": doc["timestamp"],
                    },
                    {"$set": doc, "$setOnInsert": {"created_at": datetime.utcnow()}},
                    upsert=True,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to persist industry breadth record %s: %s", doc.get("symbol"), exc)

    @staticmethod
    def _normalize_stock_code(code: str) -> str:
        token = code.strip().upper().replace(".", "").replace("SZ", "").replace("SH", "").replace("BJ", "")
        if not token or len(token) < 6:
            return ""
        if token.startswith(("60", "68", "56", "66")):
            return f"sh.{token[:6]}"
        if token.startswith(("00", "30")):
            return f"sz.{token[:6]}"
        if token.startswith(("43", "83", "87")):
            return f"bj.{token[:6]}"
        return f"sz.{token[:6]}"


__all__ = ["IndustryBreadthCalculator"]
