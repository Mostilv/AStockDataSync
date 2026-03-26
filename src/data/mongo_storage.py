from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from pymongo import ASCENDING, MongoClient, UpdateOne
from pymongo.errors import OperationFailure
from pymongo.collection import Collection
from pymongo.database import Database


@dataclass(frozen=True)
class MongoCollections:
    stock_basic: str = "stock_basic"
    stock_kline: str = "stock_kline"
    sync_meta: str = "sync_meta"


class MongoStorage:
    def __init__(
        self,
        uri: str,
        database_name: str,
        *,
        collections: Optional[MongoCollections] = None,
    ) -> None:
        self.client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        self.database: Database = self.client[database_name]
        self.collections = collections or MongoCollections()

    def ping(self) -> None:
        self.client.admin.command("ping")

    @property
    def stock_basic_collection(self) -> Collection:
        return self.database[self.collections.stock_basic]

    @property
    def stock_kline_collection(self) -> Collection:
        return self.database[self.collections.stock_kline]

    @property
    def sync_meta_collection(self) -> Collection:
        return self.database[self.collections.sync_meta]

    def ensure_indexes(self) -> None:
        self._safe_create_index(
            self.stock_basic_collection,
            [("symbol", ASCENDING)],
            unique=True,
            name="symbol_unique",
        )
        self._safe_create_index(
            self.stock_kline_collection,
            [("symbol", ASCENDING), ("frequency", ASCENDING), ("timestamp", ASCENDING)],
            unique=True,
            name="symbol_frequency_timestamp_unique",
        )
        self._safe_create_index(
            self.stock_kline_collection,
            [("symbol", ASCENDING), ("frequency", ASCENDING), ("trade_date", ASCENDING)],
            name="symbol_frequency_trade_date_idx",
        )
        self._safe_create_index(
            self.sync_meta_collection,
            [("task", ASCENDING), ("scope", ASCENDING)],
            unique=True,
            name="task_scope_unique",
        )

    def upsert_stock_basic(self, records: Iterable[Dict[str, Any]]) -> Dict[str, int]:
        operations: List[UpdateOne] = []
        now = datetime.utcnow()
        for record in records:
            symbol = record.get("symbol")
            if not symbol:
                continue
            document = {**record, "updated_at": now}
            operations.append(
                UpdateOne(
                    {"symbol": symbol},
                    {"$set": document, "$setOnInsert": {"created_at": now}},
                    upsert=True,
                )
            )
        return self._bulk_write(self.stock_basic_collection, operations)

    def upsert_stock_kline(self, records: Iterable[Dict[str, Any]]) -> Dict[str, int]:
        operations: List[UpdateOne] = []
        now = datetime.utcnow()
        for record in records:
            symbol = record.get("symbol")
            frequency = record.get("frequency")
            timestamp = record.get("timestamp")
            if not symbol or not frequency or not timestamp:
                continue
            document = {**record, "updated_at": now}
            operations.append(
                UpdateOne(
                    {
                        "symbol": symbol,
                        "frequency": frequency,
                        "timestamp": timestamp,
                    },
                    {"$set": document, "$setOnInsert": {"created_at": now}},
                    upsert=True,
                )
            )
        return self._bulk_write(self.stock_kline_collection, operations)

    def update_sync_meta(self, task: str, scope: str, payload: Dict[str, Any]) -> None:
        self.sync_meta_collection.update_one(
            {"task": task, "scope": scope},
            {
                "$set": {
                    **payload,
                    "task": task,
                    "scope": scope,
                    "updated_at": datetime.utcnow(),
                },
                "$setOnInsert": {"created_at": datetime.utcnow()},
            },
            upsert=True,
        )

    def get_sync_meta(self, task: str, scope: str) -> Optional[Dict[str, Any]]:
        return self.sync_meta_collection.find_one({"task": task, "scope": scope})

    def list_symbols(self, *, limit: Optional[int] = None) -> List[str]:
        cursor = self.stock_basic_collection.find({}, {"symbol": 1}).sort("symbol", ASCENDING)
        if limit:
            cursor = cursor.limit(limit)
        return [item["symbol"] for item in cursor if item.get("symbol")]

    def close(self) -> None:
        self.client.close()

    @staticmethod
    def _bulk_write(collection: Collection, operations: List[UpdateOne]) -> Dict[str, int]:
        if not operations:
            return {"matched": 0, "modified": 0, "upserted": 0}
        result = collection.bulk_write(operations, ordered=False)
        return {
            "matched": int(getattr(result, "matched_count", 0)),
            "modified": int(getattr(result, "modified_count", 0)),
            "upserted": len(getattr(result, "upserted_ids", {}) or {}),
        }

    @staticmethod
    def _safe_create_index(collection: Collection, keys, **kwargs) -> None:
        try:
            collection.create_index(keys, **kwargs)
        except OperationFailure as exc:
            if exc.code == 85 or "already exists with a different name" in str(exc):
                return
            raise
