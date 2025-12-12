"""Example: compute MACD for the SSE Composite Index and save to MongoDB.

Steps covered:
1) Read data: load `sh.000001` daily closes from the Baostock daily collection.
2) Calculate: run TA-Lib MACD(12, 26, 9) on the close series.
3) Save: upsert MACD values into an `indicator_data` collection.
"""

import logging
from datetime import datetime, time
from typing import List

import pandas as pd
import talib
from pymongo import MongoClient

from src.utils.config_loader import load_config

logger = logging.getLogger(__name__)


def load_sh_index_history(daily_col, code: str) -> pd.DataFrame:
    """Load SSE Composite daily closes from Mongo."""
    cursor = (
        daily_col.find({"code": code}, {"_id": 0, "date": 1, "close": 1})
        .sort("date", 1)
    )
    df = pd.DataFrame(list(cursor))
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["date", "close"])
    if df.empty:
        raise RuntimeError(f"No history found for {code} in daily collection.")
    return df


def compute_macd(df: pd.DataFrame) -> pd.DataFrame:
    """Append MACD values to dataframe."""
    macd, signal, hist = talib.MACD(
        df["close"].values,
        fastperiod=12,
        slowperiod=26,
        signalperiod=9,
    )
    df = df.assign(macd=macd, signal=signal, hist=hist)
    df = df.dropna(subset=["macd", "signal", "hist"])
    if df.empty:
        raise RuntimeError("MACD series is empty after calculation.")
    return df


def build_records(df: pd.DataFrame, indicator: str, symbol: str) -> List[dict]:
    """Convert dataframe rows to indicator documents."""
    records: List[dict] = []
    for _, row in df.iterrows():
        ts = datetime.combine(pd.to_datetime(row["date"]).date(), time())
        records.append(
            {
                "indicator": indicator,
                "symbol": symbol,
                "timeframe": "1d",
                "timestamp": ts.isoformat(),
                "value": float(row["macd"]),
                "values": {
                    "macd": float(row["macd"]),
                    "signal": float(row["signal"]),
                    "hist": float(row["hist"]),
                    "close": float(row["close"]),
                },
                "payload": {
                    "source": "macd_demo",
                    "code": "sh.000001",
                },
                "tags": ["demo", "macd", "index"],
            }
        )
    return records


def persist_records(indicator_col, records: List[dict]) -> None:
    """Upsert indicator documents."""
    if not records:
        logger.info("No MACD records to save.")
        return
    for doc in records:
        indicator_col.update_one(
            {
                "indicator": doc["indicator"],
                "symbol": doc["symbol"],
                "timeframe": doc["timeframe"],
                "timestamp": doc["timestamp"],
            },
            {
                "$set": doc,
                "$setOnInsert": {"created_at": datetime.utcnow()},
            },
            upsert=True,
        )
    logger.info("Saved %d MACD records.", len(records))


def main(config_path: str = "config.yaml") -> None:
    """Run the MACD demo end-to-end."""
    config = load_config(config_path)
    mongo_cfg = config["mongodb"]
    baostock_cfg = config["baostock"]
    client = MongoClient(mongo_cfg["uri"])
    try:
        db = client[baostock_cfg["db"]]
        daily_col = db[baostock_cfg["daily"]]
        indicator_col = db.get_collection("indicator_data")

        history = load_sh_index_history(daily_col, code="sh.000001")
        macd_df = compute_macd(history)
        docs = build_records(macd_df, indicator="macd_demo_sh", symbol="INDEX:SH000001")
        persist_records(indicator_col, docs)
    finally:
        client.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
