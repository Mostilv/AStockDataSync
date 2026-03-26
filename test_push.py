from src.config import load_runtime_config
from src.data.raw_sync import RawDataSyncService


def main() -> None:
    config = load_runtime_config()
    service = RawDataSyncService(config)
    print("Loaded config for database:", config["mongodb"]["database"])
    print("AStock frequencies:", config.get("astock", {}).get("frequencies"))
    service.close()


if __name__ == "__main__":
    main()
