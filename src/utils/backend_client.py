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
        backend_cfg = config.get("backend", {})
        self.enabled = bool(backend_cfg.get("enabled", False))
        self.base_url = backend_cfg.get("url", "http://localhost:8000").rstrip("/")
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
            
            # Adapt loop
            adapted_items = []
            for item in items:
                # Baostock: code, code_name, ipoDate, outDate, type, status
                # Backend: symbol, name, exchange, list_date, ...
                try:
                    adapted_items.append({
                        "symbol": item.get("code"),  # e.g. sh.600000
                        "name": item.get("code_name"),
                        "exchange": item.get("code", "")[:2].upper(), # sh/sz
                        "list_date": item.get("ipoDate") or None,
                        "delist_date": item.get("outDate") or None,
                        "status": "active" if item.get("status") == "1" else "delisted",
                        "type": "stock" if item.get("type") == "1" else "index", # 1=stock, 2=index
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
                # Baostock: date, code, open, high, low, close, volume, amount, adjustflag, ...
                # Akshare real: symbol, date, open, high, low, close, volume...
                
                # Unify keys
                # If date and time are separate (minute data in baostock), combine them
                ts_str = k.get("date")
                if "time" in k and k["time"]:
                    # Baostock 5m time format YYYYMMDDHHMMSSssss
                    # but date is YYYY-MM-DD
                    # Let's check format.
                    # manager_baostock.py: fields = "date,time,code..."
                    # Actually standard baostock returns 14-digit string for time?
                    # Let's look at manager_baostock.py processing. 
                    # It just passes dict.
                    # Simple approach: If 'time' exists and is long, use it as timestamp.
                    # Else combine date + time if possible.
                    # Or if akshare, it has 'datetime' object sometimes.
                    pass
                
                # Robust timestamp parsing
                timestamp = None
                if isinstance(k.get("datetime"), datetime): 
                    timestamp = k["datetime"]
                elif k.get("time") and len(str(k["time"])) > 8:
                    # e.g. 20200101103000
                    try: 
                        t_str = str(k["time"])[:14]
                        timestamp = datetime.strptime(t_str, "%Y%m%d%H%M%S")
                    except ValueError:
                        pass
                elif k.get("date"):
                    try:
                        timestamp = datetime.strptime(k["date"], "%Y-%m-%d")
                        # For minute bars without full timestamp, this might be issue.
                        # But Baostock usually provides time field for minutes.
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
