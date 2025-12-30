import sys
import os
import logging
from datetime import datetime

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from src.utils.backend_client import BackendClient
from src.utils.config_loader import load_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_backend_client():
    # Resolve config path relative to this script
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, "..", "config.yaml")
    config_path = os.path.abspath(config_path)
    
    config = load_config(config_path)
    client = BackendClient(config)
    
    # Force enable for testing even if auth might fail
    client.enabled = True
    
    print("Testing BackendClient...")
    print(f"URL: {client.base_url}")
    print(f"Username: {client.username}")
    
    # 1. Test Stock Basic
    print("\n--- Testing push_stock_basic ---")
    dummy_stocks = [
        {
            "code": "sh.600000",
            "code_name": "浦发银行",
            "ipoDate": "1999-11-10",
            "outDate": "",
            "type": "1",
            "status": "1"
        }
    ]
    try:
        client.push_stock_basic(dummy_stocks)
        print("push_stock_basic executed (check logs for success/fail).")
    except Exception as e:
        print(f"push_stock_basic failed with exception (expected if no backend): {e}")

    # 2. Test Kline
    print("\n--- Testing push_kline ---")
    dummy_kline = [
        {
            "code": "sh.600000",
            "date": "2023-01-01",
            "open": "10.0",
            "high": "10.5",
            "low": "9.5",
            "close": "10.2",
            "volume": "1000",
            "amount": "10000",
            "pctChg": "2.0"
        }
    ]
    try:
        client.push_kline(dummy_kline, frequency="d")
        print("push_kline executed.")
    except Exception as e:
         print(f"push_kline failed with exception: {e}")

    # 3. Test Indicators
    print("\n--- Testing push_indicators ---")
    dummy_indicators = [
        {
            "symbol": "sh.600000",
            "indicator": "rsi14",
            "timeframe": "1d",
            "timestamp": datetime.now(),
            "value": 50.5,
            "tags": ["test"]
        }
    ]
    try:
        client.push_indicators(dummy_indicators)
        print("push_indicators executed.")
    except Exception as e:
        print(f"push_indicators failed with exception: {e}")

if __name__ == "__main__":
    test_backend_client()
