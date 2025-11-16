import datetime as _dt
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

import requests
from bson import Decimal128, ObjectId
from pymongo import ASCENDING, MongoClient
from pymongo.collection import Collection

from ..indicators.industry_metrics import IndustryMetricsCollector
from ..utils.config_loader import load_config


DATE_FMT = "%Y-%m-%d"


class BackendSyncError(RuntimeError):
    """Raised when synchronizing data to stock_middle_platform_backend fails."""


class StockMiddlePlatformBackendSync:
    """Upload Baostock data to stock_middle_platform_backend with login enforcement."""

    def __init__(self, config_path: str = "config.yaml") -> None:
        self.config = load_config(config_path)
        backend_cfg = self.config.get("stock_middle_platform_backend")
        if not backend_cfg:
            raise BackendSyncError("缺少 stock_middle_platform_backend 配置。")

        required_keys = ("base_url", "username", "password")
        missing = [key for key in required_keys if key not in backend_cfg]
        if missing:
            raise BackendSyncError(f"backend 配置缺少字段: {', '.join(missing)}")

        self.backend_cfg = backend_cfg
        self.base_url = backend_cfg["base_url"].rstrip("/")
        self.login_path = backend_cfg.get("login_path", "/api/auth/login")
        self.basic_path = backend_cfg.get("basic_path", "/api/stocks/basic")
        self.kline_path = backend_cfg.get("kline_path", "/api/stocks/kline")
        self.timeout = float(backend_cfg.get("timeout", 10))
        self.verify_ssl = bool(backend_cfg.get("verify_ssl", True))
        self.batch_size = int(backend_cfg.get("batch_size", 500) or 500)
        self.token_header = backend_cfg.get("token_header", "Authorization")
        self.token_prefix = backend_cfg.get("token_prefix", "Bearer")
        self.token_field = backend_cfg.get("token_field", "token")
        self.basic_payload_key = backend_cfg.get("basic_payload_key", "items")
        self.kline_payload_key = backend_cfg.get("kline_payload_key", "items")
        self.extra_basic_payload = backend_cfg.get("basic_extra_payload", {}) or {}
        self.extra_kline_payload = backend_cfg.get("kline_extra_payload", {}) or {}
        self.provider = backend_cfg.get("provider", "astock-sync")
        self.basic_target = backend_cfg.get("basic_target", "primary")
        self.kline_target = backend_cfg.get("kline_target", "primary")
        self.indicator_target = backend_cfg.get("indicator_target", "primary")
        self.indicator_path = backend_cfg.get("indicator_path", "/api/indicators/records")
        self.indicator_provider = backend_cfg.get("indicator_provider", self.provider)
        self.industry_metrics_cfg = backend_cfg.get("industry_metrics", {}) or {}

        mongo_cfg = self.config.get("mongodb")
        if not mongo_cfg:
            raise BackendSyncError("缺少 mongodb 配置。")
        baostock_cfg = self.config.get("baostock")
        if not baostock_cfg:
            raise BackendSyncError("缺少 baostock 配置。")

        self.mongo_client = MongoClient(mongo_cfg["uri"])
        baostock_db = self.mongo_client[baostock_cfg["db"]]
        self.basic_collection = baostock_db[baostock_cfg["basic"]]
        self.kline_collections: Dict[str, Collection] = {
            "d": baostock_db[baostock_cfg["daily"]],
            "w": baostock_db[baostock_cfg.get("weekly", "weekly_adjusted")],
            "m": baostock_db[baostock_cfg.get("monthly", "monthly_adjusted")],
            "15": baostock_db[baostock_cfg["minute_15"]],
            "60": baostock_db[baostock_cfg["minute_60"]],
        }

        self.session = requests.Session()
        self.session.verify = self.verify_ssl
        self._token: Optional[str] = None
        self._login_payload = {
            **backend_cfg.get("login_payload", {}),
            "username": backend_cfg["username"],
            "password": backend_cfg["password"],
        }

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #
    def push_stock_basic(self, batch_size: Optional[int] = None, limit: Optional[int] = None) -> int:
        """Send stock_basic collection to backend."""
        batch = int(batch_size or self.batch_size)
        cursor = self.basic_collection.find({})
        cursor = cursor.sort([("code", ASCENDING)])
        if limit:
            cursor = cursor.limit(int(limit))
        total = 0
        for docs in self._batched(self._sanitize_cursor(cursor), batch):
            items = [
                payload
                for payload in (self._transform_basic_doc(doc) for doc in docs)
                if payload
            ]
            if not items:
                continue
            payload = {
                "target": self.basic_target,
                "provider": self.provider,
                self.basic_payload_key: items,
                **self.extra_basic_payload,
            }
            self._post_json(self.basic_path, payload)
            total += len(items)
        return total

    def push_kline(
        self,
        frequency: str = "d",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        batch_size: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> int:
        """Send k-line data (daily/weekly/monthly/minute) to backend."""
        freq = frequency.lower()
        if freq not in self.kline_collections:
            raise BackendSyncError(f"不支持的 frequency: {frequency}")

        batch = int(batch_size or self.batch_size)
        query: Dict[str, Dict[str, str]] = {}
        start = self._normalize_date(start_date) if start_date else None
        end = self._normalize_date(end_date) if end_date else None
        if start or end:
            date_filter: Dict[str, str] = {}
            if start:
                date_filter["$gte"] = start
            if end:
                date_filter["$lte"] = end
            query["date"] = date_filter

        collection = self.kline_collections[freq]
        sort_fields: List[Tuple[str, int]] = [("date", ASCENDING)]
        if freq in ("15", "60"):
            sort_fields.append(("time", ASCENDING))

        cursor = collection.find(query)
        cursor = cursor.sort(sort_fields)
        if limit:
            cursor = cursor.limit(int(limit))

        total = 0
        for docs in self._batched(self._sanitize_cursor(cursor), batch):
            items = [
                payload
                for payload in (self._transform_kline_doc(doc, freq) for doc in docs)
                if payload
            ]
            if not items:
                continue
            payload = {
                "target": self.kline_target,
                "provider": self.provider,
                self.kline_payload_key: items,
                **self.extra_kline_payload,
            }
            self._post_json(self.kline_path, payload)
            total += len(items)
        return total

    def push_industry_metrics(
        self,
        lookback_days: Optional[int] = None,
        industry_limit: Optional[int] = None,
        codes: Optional[List[str]] = None,
    ) -> int:
        """Collect Shenwan industry metrics via Akshare and push to backend indicator API."""
        cfg = self.industry_metrics_cfg
        collector = IndustryMetricsCollector(
            lookback_days=lookback_days
            or int(cfg.get("lookback_days", 12) or 12),
            momentum_period=int(cfg.get("momentum_period", 5) or 5),
            industry_limit=industry_limit
            or int(cfg.get("industry_limit", 0) or 0)
            or None,
            codes=codes or cfg.get("codes"),
        )
        records = collector.collect()
        total = 0
        for chunk in self._batched(records, self.batch_size):
            payload = {
                "target": self.indicator_target,
                "provider": self.indicator_provider,
                "records": chunk,
            }
            self._post_json(self.indicator_path, payload)
            total += len(chunk)
        return total

    def close(self) -> None:
        """Release network & database resources."""
        self.session.close()
        self.mongo_client.close()

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _build_url(self, path: str) -> str:
        if path.startswith("http://") or path.startswith("https://"):
            return path
        return f"{self.base_url}{'' if path.startswith('/') else '/'}{path}"

    def _ensure_authenticated(self) -> None:
        if self._token:
            return
        self._login()

    def _login(self) -> None:
        url = self._build_url(self.login_path)
        response = self.session.post(url, json=self._login_payload, timeout=self.timeout)
        if response.status_code >= 400:
            raise BackendSyncError(f"登录 stock_middle_platform_backend 失败: {response.text}")

        data = response.json()
        token = data.get(self.token_field)
        if not token:
            raise BackendSyncError(f"登录响应中找不到 {self.token_field} 字段。")
        self._token = token
        header_value = f"{self.token_prefix} {token}".strip()
        self.session.headers[self.token_header] = header_value

    def _post_json(self, path: str, payload: Dict) -> Dict:
        self._ensure_authenticated()
        url = self._build_url(path)
        response = self.session.post(url, json=payload, timeout=self.timeout)
        if response.status_code == 401:
            # Token expired: attempt once more after re-login.
            self._token = None
            self.session.headers.pop(self.token_header, None)
            self._login()
            response = self.session.post(url, json=payload, timeout=self.timeout)
        if response.status_code >= 400:
            raise BackendSyncError(
                f"请求 {url} 失败，状态码 {response.status_code}，响应: {response.text}"
            )
        if response.content:
            try:
                return response.json()
            except ValueError:
                return {}
        return {}

    def _sanitize_cursor(self, cursor: Iterable[Dict]) -> Iterator[Dict]:
        for doc in cursor:
            yield self._sanitize_document(doc)

    def _sanitize_document(self, document: Dict) -> Dict:
        sanitized: Dict = {}
        for key, value in document.items():
            if key == "_id":
                continue
            if isinstance(value, (ObjectId,)):
                sanitized[key] = str(value)
            elif isinstance(value, Decimal128):
                sanitized[key] = float(value.to_decimal())
            elif isinstance(value, _dt.datetime):
                sanitized[key] = value.strftime("%Y-%m-%d %H:%M:%S")
            elif isinstance(value, _dt.date):
                sanitized[key] = value.strftime(DATE_FMT)
            else:
                sanitized[key] = value
        return sanitized

    def _transform_basic_doc(self, document: Dict) -> Optional[Dict]:
        raw_code = document.get("code") or document.get("symbol")
        if not raw_code:
            return None
        symbol = self._normalize_symbol(raw_code)
        if not symbol:
            return None
        payload = self._clean_payload(document)
        record = {
            "symbol": symbol,
            "name": document.get("code_name") or document.get("name") or symbol,
            "exchange": symbol[:2],
            "list_date": self._parse_date_field(document.get("ipoDate")),
            "delist_date": self._parse_date_field(document.get("outDate")),
            "status": document.get("status"),
            "type": document.get("type"),
            "market": document.get("market"),
            "industry": document.get("industry"),
            "payload": payload,
        }
        return {
            key: value
            for key, value in record.items()
            if value not in (None, "", {})
        }

    def _transform_kline_doc(self, document: Dict, frequency: str) -> Optional[Dict]:
        raw_code = document.get("code") or document.get("symbol")
        if not raw_code:
            return None
        symbol = self._normalize_symbol(raw_code)
        try:
            trade_date = self._normalize_date(document.get("date"))
        except BackendSyncError:
            return None
        if not symbol or not trade_date:
            return None
        timestamp = self._combine_timestamp(trade_date, document.get("time"))
        record = {
            "symbol": symbol,
            "frequency": frequency,
            "timestamp": timestamp,
            "open": self._to_float(document.get("open")),
            "high": self._to_float(document.get("high")),
            "low": self._to_float(document.get("low")),
            "close": self._to_float(document.get("close")),
            "volume": self._to_float(document.get("volume")),
            "amount": self._to_float(document.get("amount")),
            "turnover_rate": self._to_float(document.get("turn")),
            "adjust_flag": document.get("adjustflag"),
            "trade_status": self._normalize_trade_status(document.get("tradestatus")),
            "pct_change": self._to_float(document.get("pctChg")),
            "pe_ttm": self._to_float(document.get("peTTM")),
            "pb_mrq": self._to_float(document.get("pbMRQ")),
            "ps_ttm": self._to_float(document.get("psTTM")),
            "pcf_ncf_ttm": self._to_float(document.get("pcfNcfTTM")),
            "payload": self._clean_payload(document),
        }
        required_keys = ("open", "high", "low", "close", "volume")
        if any(record[key] is None for key in required_keys):
            return None
        return {
            key: value
            for key, value in record.items()
            if value not in (None, "", {})
        }

    def _clean_payload(self, document: Dict) -> Dict:
        payload: Dict[str, Any] = {}
        for key, value in document.items():
            if key == "_id":
                continue
            if isinstance(value, str) and not value.strip():
                continue
            payload[key] = value
        return payload

    def _batched(self, iterator: Iterable[Dict], size: int) -> Iterator[List[Dict]]:
        batch: List[Dict] = []
        for item in iterator:
            batch.append(item)
            if len(batch) >= size:
                yield batch
                batch = []
        if batch:
            yield batch

    def _normalize_date(self, date_str: str) -> str:
        if not date_str:
            raise BackendSyncError("日期不能为空。")
        if "-" in date_str:
            return date_str
        if len(date_str) == 8 and date_str.isdigit():
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        raise BackendSyncError(f"无法解析日期格式: {date_str}")

    def _parse_date_field(self, value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        try:
            return self._normalize_date(str(value))
        except BackendSyncError:
            return None

    def _combine_timestamp(
        self, date_str: str, time_str: Optional[str]
    ) -> str:
        if not time_str:
            return f"{date_str}T00:00:00"
        clean = str(time_str).strip()
        if ":" not in clean and len(clean) == 4:
            clean = f"{clean[:2]}:{clean[2:]}:00"
        if ":" in clean and len(clean.split(":")) == 2:
            clean = f"{clean}:00"
        return f"{date_str}T{clean}"

    def _normalize_symbol(self, code: str) -> str:
        token = (code or "").strip().upper()
        if not token:
            return ""
        token = token.replace(".", "").replace("-", "")
        if token.startswith("SH") or token.startswith("SZ") or token.startswith("BJ"):
            return token
        if token.startswith("0") or token.startswith("3"):
            return f"SZ{token}"
        if token.startswith("6"):
            return f"SH{token}"
        return token

    def _normalize_trade_status(self, status: Optional[Any]) -> Optional[str]:
        if status is None:
            return None
        text = str(status).strip().lower()
        if text in {"1", "true", "trading", "open"}:
            return "trading"
        if text in {"0", "false", "halted", "suspend"}:
            return "halted"
        return None

    def _to_float(self, value: Optional[Any]) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip()
        if not text or text.lower() in {"none", "nan"}:
            return None
        try:
            return float(text)
        except ValueError:
            return None

    def __enter__(self) -> "StockMiddlePlatformBackendSync":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()
