# AStockDataSync

`AStockDataSync` now acts as the raw data ingestion project.

Responsibilities:

- fetch raw stock data from `akshare`
- apply light normalization and cleaning
- write cleaned raw data directly into MongoDB
- maintain sync metadata for local database refresh tasks

Default collections:

- `stock_basic`
- `stock_kline`
- `stock_fundamental`
- `sync_meta`

## Commands

Sync stock basics:

```bash
python main.py basic
```

Sync a K-line frequency:

```bash
python main.py kline --frequency d --days 180
```

Sync market-wide fundamentals:

```bash
python main.py fundamental --periods 8
```

Sync only the latest report-period fundamentals:

```bash
python main.py fundamental-latest
```

Run full maintenance:

```bash
python run_sync.py
```

Run periodic maintenance:

```bash
python maintain_loop.py
```

## Config

See `config.yaml`.

Important keys:

- `mongodb.uri`
- `mongodb.database`
- `mongodb.collections.*`
- `astock.frequencies`
- `astock.daily_lookback_days`
- `astock.fundamental_report_periods`
- `astock.fundamental_maintain_periods`
- `astock.maintain_interval_minutes`
