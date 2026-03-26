from __future__ import annotations

from datetime import date, datetime, time
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd


def normalize_symbol(raw_symbol: str) -> str:
    text = str(raw_symbol or "").strip()
    if not text:
        raise ValueError("symbol is required")
    if text.startswith(("SH", "SZ", "BJ")):
        return text.upper()
    if text.startswith(("6",)):
        return f"SH{text}"
    if text.startswith(("0", "3")):
        return f"SZ{text}"
    if text.startswith(("4", "8")):
        return f"BJ{text}"
    return text.upper()


def raw_symbol(symbol: str) -> str:
    value = normalize_symbol(symbol)
    if len(value) > 2 and value[:2] in {"SH", "SZ", "BJ"}:
        return value[2:]
    return value


def clean_stock_basic(df: pd.DataFrame) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []

    renamed = df.rename(
        columns={
            "代码": "code",
            "名称": "name",
            "所处行业": "industry",
            "地区": "area",
            "上市时间": "list_date",
        }
    )
    records: List[Dict[str, Any]] = []
    for item in renamed.to_dict(orient="records"):
        code = str(item.get("code") or "").strip()
        name = str(item.get("name") or "").strip()
        if not code or not name:
            continue
        symbol = normalize_symbol(code)
        list_date = _parse_date(item.get("list_date"))
        records.append(
            {
                "symbol": symbol,
                "name": name,
                "exchange": symbol[:2],
                "status": "active",
                "type": "stock",
                "industry": _clean_optional_text(item.get("industry")),
                "area": _clean_optional_text(item.get("area")),
                "list_date": list_date,
                "currency": "CNY",
                "provider": "akshare",
                "payload": {key: _clean_scalar(value) for key, value in item.items()},
            }
        )
    return records


def clean_kline(
    symbol: str,
    frequency: str,
    df: pd.DataFrame,
) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []

    normalized_symbol = normalize_symbol(symbol)
    normalized_frequency = str(frequency).strip().lower()
    renamed = df.rename(
        columns={
            "日期": "trade_time",
            "时间": "trade_time",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "volume",
            "成交额": "amount",
            "振幅": "amplitude",
            "涨跌幅": "pct_change",
            "涨跌额": "pct_amount",
            "换手率": "turnover_rate",
        }
    )

    records: List[Dict[str, Any]] = []
    for row in renamed.to_dict(orient="records"):
        timestamp = _parse_datetime(row.get("trade_time"))
        if timestamp is None:
            continue
        open_price = _to_float(row.get("open"))
        high_price = _to_float(row.get("high"))
        low_price = _to_float(row.get("low"))
        close_price = _to_float(row.get("close"))
        volume = _to_float(row.get("volume"), default=0.0)
        if None in {open_price, high_price, low_price, close_price}:
            continue

        records.append(
            {
                "symbol": normalized_symbol,
                "frequency": normalized_frequency,
                "timestamp": timestamp,
                "trade_date": datetime.combine(timestamp.date(), time.min),
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": volume or 0.0,
                "amount": _to_float(row.get("amount")),
                "turnover_rate": _to_float(row.get("turnover_rate")),
                "pct_change": _to_float(row.get("pct_change")),
                "provider": "akshare",
                "payload": {key: _clean_scalar(value) for key, value in row.items()},
            }
        )
    return records


def chunked(items: Iterable[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    buffer: List[Dict[str, Any]] = []
    for item in items:
        buffer.append(item)
        if len(buffer) >= size:
            yield buffer
            buffer = []
    if buffer:
        yield buffer


def _parse_date(value: Any) -> Optional[datetime]:
    if value in (None, "", "nan"):
        return None
    text = str(value).strip()
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _parse_datetime(value: Any) -> Optional[datetime]:
    if value in (None, "", "nan"):
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    text = str(value).strip()
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%Y%m%d",
        "%Y-%m-%d %H:%M",
    ):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def _to_float(value: Any, *, default: Optional[float] = None) -> Optional[float]:
    if value is None:
        return default
    try:
        if pd.isna(value):
            return default
    except TypeError:
        pass
    text = str(value).replace(",", "").strip()
    if not text:
        return default
    try:
        return float(text)
    except ValueError:
        return default


def _clean_optional_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    return text


def _clean_scalar(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return datetime.combine(value, time.min).isoformat()
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    return value
