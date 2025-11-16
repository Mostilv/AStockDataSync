import logging
from datetime import datetime, time
from typing import Any, Dict, Iterable, List, Optional

import akshare as ak
import pandas as pd


logger = logging.getLogger(__name__)


class IndustryMetricsCollector:
    """Use Akshare Shenwan index data to compute industry momentum/width metrics."""

    def __init__(
        self,
        lookback_days: int = 12,
        momentum_period: int = 5,
        industry_limit: Optional[int] = None,
        codes: Optional[Iterable[str]] = None,
    ) -> None:
        self.lookback_days = max(1, int(lookback_days))
        self.momentum_period = max(1, int(momentum_period))
        self.industry_limit = (
            max(1, int(industry_limit)) if industry_limit else None
        )
        self.codes = [code.strip() for code in codes or [] if code.strip()]

    def collect(self) -> List[Dict[str, Any]]:
        industries = self._load_industry_metadata()
        results: List[Dict[str, Any]] = []
        for info in industries:
            history = self._load_history(info["code"])
            if history is None or history.empty:
                continue
            history = history.sort_values("日期").reset_index(drop=True)
            history["momentum"] = (
                history["收盘"].pct_change(periods=self.momentum_period) * 100
            )
            with pd.option_context("mode.use_inf_as_na", True):
                history["width"] = (
                    (history["最高"] - history["最低"]) / history["收盘"]
                ) * 100
            window = history.tail(self.lookback_days)
            for _, row in window.iterrows():
                dt_value = row["日期"]
                if pd.isna(dt_value):
                    continue
                timestamp = datetime.combine(
                    pd.to_datetime(dt_value).date(), time()
                )
                record = {
                    "indicator": "industry_metrics",
                    "symbol": f"INDUSTRY:{info['code']}",
                    "timeframe": "1d",
                    "timestamp": timestamp.isoformat(),
                    "values": {
                        "momentum": self._safe_float(row.get("momentum")),
                        "width": self._safe_float(row.get("width")),
                    },
                    "payload": {
                        "industry_code": info["code"],
                        "industry_name": info["name"],
                        "close": self._safe_float(row.get("收盘")),
                        "high": self._safe_float(row.get("最高")),
                        "low": self._safe_float(row.get("最低")),
                        "turnover": self._safe_float(row.get("成交额")),
                    },
                    "tags": ["industry", "momentum", "width"],
                }
                results.append(record)
        return results

    def _load_industry_metadata(self) -> List[Dict[str, str]]:
        try:
            df = ak.sw_index_first_info()
        except Exception as exc:
            logger.error("无法获取申万行业列表: %s", exc)
            return []
        df = df[["行业代码", "行业名称"]].dropna()
        data = [
            {"code": str(row["行业代码"]).strip(), "name": str(row["行业名称"]).strip()}
            for _, row in df.iterrows()
        ]
        if self.codes:
            filtered = [item for item in data if item["code"] in self.codes]
        else:
            filtered = data
        if self.industry_limit:
            filtered = filtered[: self.industry_limit]
        return filtered

    def _load_history(self, code: str) -> Optional[pd.DataFrame]:
        try:
            df = ak.index_hist_sw(symbol=code, period="day")
        except Exception as exc:
            logger.warning("获取行业 %s 历史数据失败: %s", code, exc)
            return None
        if df is None or df.empty:
            return None
        df["日期"] = pd.to_datetime(df["日期"], errors="coerce").dt.date
        numeric_cols = ["收盘", "最高", "最低", "成交额"]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        return df

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
