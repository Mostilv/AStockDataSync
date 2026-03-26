import argparse
import logging
import os

from src.data.fast_sync import fetch_market_indices, fetch_limit_up_pool
from src.utils.backend_client import BackendClient
from src.utils.config_loader import load_config
from src.indicators.registry import run_indicator_suite

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml")

def main() -> None:
    parser = argparse.ArgumentParser(description="Simplified AStock Sync Runner")
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="Path to config.yaml")
    args = parser.parse_args()

    config = load_config(args.config)
    backend_client = BackendClient(config)

    logger.info("=== 1. Fetching & Pushing Market Indices ===")
    indices_data = fetch_market_indices(lookback_days=5)
    if indices_data:
        backend_client.push_market_indices(indices_data)
        logger.info(f"Pushed indices: {list(indices_data.keys())}")
    else:
        logger.warning("No indices data fetched.")

    logger.info("=== 2. Fetching & Pushing Limit Up Pool ===")
    limit_up_pool = fetch_limit_up_pool()
    if limit_up_pool:
        # Pass today's date or the date from the first record
        date_str = limit_up_pool[0].get("date", "")
        backend_client.push_limit_up_pool(date_str, limit_up_pool)
        logger.info(f"Pushed {len(limit_up_pool)} limit up records.")
    else:
        logger.warning("No limit up pool data fetched.")

    logger.info("=== 3. Fetching & Pushing Industry Metrics (Breadth & Momentum) ===")
    # Using the existing indicator framework which calculates and pushes to /indicators/records
    workflow_cfg = config.get("workflow", {}) or {}
    daily_cfg = workflow_cfg.get("daily_update") or workflow_cfg.get("daily") or {}
    indicator_cfg = daily_cfg.get("indicators", {}) or {}
    
    if indicator_cfg.get("enabled", True):
        # run_indicator_suite handles both calculations and pushing internally using its backend_client
        logger.info("Triggering native indicator suite...")
        run_indicator_suite(config_path=args.config, jobs=[], dry_run=False, backend_client=backend_client)
    else:
        logger.info("Indicator suite disabled in config.")

    logger.info("=== Sync Complete ===")

if __name__ == "__main__":
    main()
