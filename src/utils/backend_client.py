import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class BackendClient:
    """
    Client for interacting with stock_middle_platform_backend API.
    Handles authentication, token management, and data pushing.
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        # Support both 'stock_middle_platform_backend' (new) and legacy 'backend' keys
        backend_cfg = config.get("stock_middle_platform_backend", config.get("backend", {}))
        self.enabled = bool(backend_cfg.get("enabled", False))
        self.base_url = backend_cfg.get("base_url", backend_cfg.get("url", "http://localhost:8000")).rstrip("/")
        self.username = backend_cfg.get("username", "")
        self.password = backend_cfg.get("password", "")
        self.api_prefix = backend_cfg.get("api_prefix", "/api/v1")
        self.target = backend_cfg.get("target", "primary")  # Target database alias in backend
        
        self.token: Optional[str] = None
        self.token_expiry: Optional[datetime] = None
        
        # Session with retry logic
        self.session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504],
        )
        self.session.mount("http://", HTTPAdapter(max_retries=retries))
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

    def _ensure_auth(self) -> None:
        """Ensure valid JWT token exists, refresh if needed."""
        if not self.enabled:
            return

        if self.token and self.token_expiry and datetime.utcnow() < self.token_expiry:
            return

        self._login()

    def _login(self) -> None:
        if not self.username or not self.password:
            logger.warning("后端同步已开启，但未配置用户名/密码，跳过登录。")
            self.enabled = False
            return

        url = f"{self.base_url}{self.api_prefix}/auth/login"
        try:
            resp = self.session.post(
                url,
                json={"username": self.username, "password": self.password},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            self.token = data["token"]
            # Conservative expiry (e.g. assume 15 mins if not returned, or parse token)
            # Backend default is 30 mins, set refresh at 25 mins
            self.token_expiry = datetime.utcnow() + timedelta(minutes=25)
            logger.info("Successfully logged into backend.")
        except Exception as e:
            logger.error(f"Failed to login to backend: {e}")
            # Don't disable completely, retry next time might work, but for now raise or return
            # If login fails, we probably can't push data.
            raise

    def get_headers(self) -> Dict[str, str]:
        if not self.token:
            return {}
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def push_stock_basic(self, items: List[Dict[str, Any]]) -> None:
        """
        Push stock basic info. 
        Expects items to match StockBasicRecord schema roughly, 
        but we may need to adapt fields from Baostock format.
        """
        if not self.enabled or not items:
            return

        try:
            self._ensure_auth()
            url = f"{self.base_url}{self.api_prefix}/stocks/basic"
            
            # Adapt Akshare stock_basic format to backend schema
            adapted_items = []
            for item in items:
                # Akshare: code, code_name, source
                # Backend: symbol, name, exchange, ...
                code = item.get("code", "")
                try:
                    # Determine exchange from code prefix
                    if code.startswith("6"):
                        exchange = "SH"
                    elif code.startswith(("0", "3")):
                        exchange = "SZ"
                    elif code.startswith(("4", "8")):
                        exchange = "BJ"
                    else:
                        exchange = ""
                    adapted_items.append({
                        "symbol": code,
                        "name": item.get("code_name"),
                        "exchange": exchange,
                        "status": "active",
                        "type": "stock",
                        "payload": item
                    })
                except Exception:
                    continue

            if not adapted_items:
                return

            # Batching could be done here if list is huge, but basic list is usually handled in one go or few chunks
            # Backend validates batch size probably. Let's chunk conservatively.
            batch_size = 500
            for i in range(0, len(adapted_items), batch_size):
                chunk = adapted_items[i : i + batch_size]
                payload = {
                    "target": self.target,
                    "provider": "astock_local",
                    "items": chunk
                }
                resp = self.session.post(url, json=payload, headers=self.get_headers(), timeout=30)
                resp.raise_for_status()
                logger.info(f"Pushed {len(chunk)} stock basic records to backend.")

        except Exception as e:
            logger.error(f"Failed to push stock basic: {e}")

    def push_kline(self, kline_list: List[Dict[str, Any]], frequency: str) -> None:
        """
        Push K-line data.
        frequency: d, w, m, 5, 15, 30, 60
        """
        if not self.enabled or not kline_list:
            return

        try:
            self._ensure_auth()
            url = f"{self.base_url}{self.api_prefix}/stocks/kline"
            
            # Mapping freq
            freq_map = {
                "d": "d", "w": "w", "m": "m", 
                "5": "5", "15": "15", "30": "30", "60": "60"
            }
            target_freq = freq_map.get(frequency)
            if not target_freq and frequency.endswith("m"):
                 target_freq = frequency.replace("m", "")

            if not target_freq:
                logger.warning(f"Unsupported frequency {frequency} for backend push.")
                return

            adapted_items = []
            for k in kline_list:
                # Akshare format: code, date, open, high, low, close, volume, amount, adjustflag
                # For minute data: code, date, time, open, high, low, close, volume, amount

                # Robust timestamp parsing
                timestamp = None
                if isinstance(k.get("datetime"), datetime):
                    timestamp = k["datetime"]
                elif k.get("date") and k.get("time"):
                    # Akshare minute data: date="2024-01-02", time="09:30:00"
                    try:
                        timestamp = datetime.strptime(f"{k['date']} {k['time']}", "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        pass
                elif k.get("date"):
                    try:
                        timestamp = datetime.strptime(k["date"], "%Y-%m-%d")
                    except ValueError:
                        pass
                
                if not timestamp:
                    continue

                item = {
                    "symbol": k.get("code") or k.get("symbol"),
                    "frequency": str(target_freq),
                    "timestamp": timestamp.isoformat(),
                    "open": float(k.get("open", 0)),
                    "high": float(k.get("high", 0)),
                    "low": float(k.get("low", 0)),
                    "close": float(k.get("close", 0)),
                    "volume": float(k.get("volume", 0)),
                    "amount": float(k.get("amount", 0)) if k.get("amount") else None,
                    "adjust_flag": str(k.get("adjustflag")) if k.get("adjustflag") else None,
                    "pct_change": float(k.get("pctChg")) if k.get("pctChg") else None,
                    "turnover_rate": float(k.get("turn")) if k.get("turn") else None,
                    "pe_ttm": float(k.get("peTTM")) if k.get("peTTM") else None,
                    "payload": {key: val for key, val in k.items() if key not in ["date", "time", "datetime"]}
                }
                adapted_items.append(item)

            if not adapted_items:
                return

            batch_size = 1000
            for i in range(0, len(adapted_items), batch_size):
                chunk = adapted_items[i : i + batch_size]
                payload = {
                    "target": self.target,
                    "provider": "astock_local",
                    "items": chunk
                }
                resp = self.session.post(url, json=payload, headers=self.get_headers(), timeout=60)
                resp.raise_for_status()
                logger.info(f"Pushed {len(chunk)} kline records ({frequency}) to backend.")

        except Exception as e:
            logger.error(f"Failed to push kline data: {e}")

    def push_indicators(self, records: List[Dict[str, Any]]) -> None:
        """
        Push indicator records.
        """
        if not self.enabled or not records:
            return

        try:
            self._ensure_auth()
            url = f"{self.base_url}{self.api_prefix}/indicators/records"

            adapted_items = []
            for r in records:
                # Registry keys: indicator, symbol, timeframe, timestamp, value, values...
                item = {
                    "symbol": r.get("symbol"),
                    "indicator": r.get("indicator"),
                    "timeframe": r.get("timeframe", "1d"),
                    "timestamp": r.get("timestamp"),
                    "value": r.get("value"),
                    "values": r.get("values", {}),
                    "tags": r.get("tags", []),
                    "payload": {k: v for k, v in r.items() if k not in ["symbol", "indicator", "timeframe", "timestamp", "value", "values", "tags"]}
                }
                # Timestamp to iso
                if isinstance(item["timestamp"], datetime):
                    item["timestamp"] = item["timestamp"].isoformat()
                
                adapted_items.append(item)

            if not adapted_items:
                return

            batch_size = 500
            for i in range(0, len(adapted_items), batch_size):
                chunk = adapted_items[i : i + batch_size]
                payload = {
                    "target": self.target,
                    "provider": "astock_local",
                    "records": chunk
                }
                resp = self.session.post(url, json=payload, headers=self.get_headers(), timeout=30)
                resp.raise_for_status()
                logger.info(f"Pushed {len(chunk)} indicator records to backend.")

        except Exception as e:
            logger.error(f"Failed to push indicators: {e}")

    def push_market_indices(self, indices_data: Dict[str, Any]) -> None:
        if not self.enabled or not indices_data:
            return
        try:
            self._ensure_auth()
            url = f"{self.base_url}{self.api_prefix}/data/market/indices"
            payload = {
                "target": self.target,
                "data": indices_data
            }
            resp = self.session.post(url, json=payload, headers=self.get_headers(), timeout=30)
            resp.raise_for_status()
            logger.info("Pushed market indices successfully.")
        except Exception as e:
            logger.error(f"Failed to push market indices: {e}")

    def push_limit_up_pool(self, date_str: str, pool_data: List[Dict[str, Any]]) -> None:
        if not self.enabled or not pool_data:
            return
        try:
            self._ensure_auth()
            url = f"{self.base_url}{self.api_prefix}/data/limit_up/pool"
            payload = {
                "target": self.target,
                "date": date_str,
                "data": pool_data
            }
            # Batching usually not needed as limit up is < 200 stocks
            resp = self.session.post(url, json=payload, headers=self.get_headers(), timeout=30)
            resp.raise_for_status()
            logger.info(f"Pushed {len(pool_data)} limit up records for {date_str}.")
        except Exception as e:
            msg = e.response.text if hasattr(e, "response") and e.response is not None else str(e)
            logger.error(f"Failed to push limit up pool: {msg}")

    def check_integrity(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Check data integrity against backend.
        items: List of dict with keys: symbol, frequency, start_date, end_date
        """
        if not self.enabled or not items:
            return []

        try:
            self._ensure_auth()
            url = f"{self.base_url}{self.api_prefix}/integrity/check"
            
            # Ensure items have date objects serialized or strings
            # Backend expects strings YYYY-MM-DD or date objects if using jsonable_encoder
            # Here we manually construct dict.
            # Backend model needs: symbol, frequency, start_date, end_date
            
            payload = {
                "target": self.target,
                "items": items
            }
            resp = self.session.post(url, json=payload, headers=self.get_headers(), timeout=60)
            resp.raise_for_status()
            return resp.json().get("results", [])
        except Exception as e:
            logger.error(f"Failed to check integrity: {e}")
            return []
