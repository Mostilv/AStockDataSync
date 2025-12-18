# AStockDataSync 数据维护指南

AStockDataSync 是一套面向 MongoDB 的 A 股行情与财务数据维护工具，定位为“统一从公开接口获取数据并落地本地库”的入口。历史数据由 **baostock** 负责，盘中/收盘补齐由 **akshare** 提供。项目重构为 `src/` 包，可通过 CLI 或脚本灵活调用。

## 核心功能

- **BaostockManager (`src.data.manager_baostock`)**
  - 统一登录/登出，按照 `config.yaml` 建立 MongoDB 连接并创建索引。
  - `sync_k_data` 覆盖日/周/月/5m，默认日/周/月回溯近 3 年；5m 从 2019-01-02 起但仅回溯近 1 个月。
  - 支持配置 `index_codes` 将指数加入 K 线更新；内置 Baostock 调用计数防止超限。
  - `run_integrity_check` 可按窗口重新拉取近窗数据修补缺口。
- **AkshareRealtimeManager (`src.data.manager_akshare`)**
  - 基于 `stock_zh_a_spot_em` 获取实时快照，并按配置组合生成 5m K 线。
  - 支持单次补齐或循环模式，可在收盘后强制落库当前 bar。
- **IndicatorEngine (`src.indicators.technical_engine`)**
  - 读取 MongoDB 中的 K 线，按配置计算技术指标（当前示例：MACD），upsert 到 `indicator_collection`。
  - 通过 `jobs` 列表扩展更多自定义指标。
- **标签同步（行业/概念）**
  - 每日更新时自动获取申万一级行业归属、同花顺概念成分，将标签写入 `stock_basic`（可配置开关）。
- **数据源标记与覆盖**
  - 所有写库记录会带上 `source`（baostock/akshare）和 `temporary` 标记，便于区分正式与临时数据。
  - Akshare 聚合的分钟线会镜像写入 `baostock.minute_5` 作为临时数据，待 Baostock 官方口径到达后通过 upsert 自动覆盖。
- **每日维护脚本 `daily_update.py`**
  - 全量配置驱动：读取 `workflow.daily_update`，自动决定日/周/月/分钟更新频率。
  - 周线、月线按 baostock 交易日历判断是否到达周/月末；如果库里缺少周/月数据，会立即补齐，即便未到周末/月末。
  - 同步完成后自动执行标签更新（申万一级行业、概念）与指标计算（技术指标 + 行业指标），默认包含上证指数 MACD 与行业动量/宽度。
  - 提供 `--dry-run` 仅打印计划；其他开关收敛到配置文件。

CLI `python main.py` 仍可单独触发 baostock/akshare/backend 命令，默认入口推荐使用 `daily_update.py`。

## 指标计算示例：上证指数 MACD

- `config.yaml` 中 `workflow.daily_update.indicators.jobs` 默认提供 `sse_macd`，目标标的 `sh.000001`。
- `daily_update.py` 会在完成数据同步后自动计算 MACD(12,26,9)，并写入 `indicator_collection`（默认 `indicator_data`）。

## 安装与配置

```bash
pip install -r requirements.txt
```

`config.yaml` 示例（可根据库名/集合/窗口修改）：

```yaml
mongodb:
  uri: "mongodb://localhost:27017/"

baostock:
  db: "baostock"
  basic: "stock_basic"
  daily: "daily_adjusted"
  weekly: "weekly_adjusted"
  monthly: "monthly_adjusted"
  minute_5: "minute_5_adjusted"
  history_years: 3
  daily_call_limit: 150000
  minute_start_date: "2019-01-02"
  minute_lookback_days: 30
  frequencies: ["d", "w", "m", "5"]
  index_codes: ["sh.000001"]
  indicator_collection: "indicator_data"
  integrity_windows:
    d: 30
    w: 400
    m: 1500
    "5": 15

akshare:
  db: "akshare_realtime"
  kline: "kline"
  daily: "daily"
  symbols: []            # 留空时自动读取 stock_basic 中的 code
  timeframes: ["5m"]
  sleep_seconds: 5
  source_tag: "akshare"
  mirror_to_baostock: true
  force_ipv4: true

stock_middle_platform_backend:
  base_url: "http://localhost:8000/api/v1"
  login_path: "/auth/login"
  basic_path: "/stocks/basic"
  kline_path: "/stocks/kline"
  indicator_path: "/indicators/records"
  username: "please-set-me"
  password: "please-set-me"
  verify_ssl: true
  timeout: 10
  batch_size: 500
  token_field: "token"
  token_prefix: "Bearer"
  token_header: "Authorization"
  provider: "astock-sync"
  basic_target: "primary"
  kline_target: "primary"
  indicator_target: "primary"
  industry_metrics:
    lookback_days: 12
    momentum_period: 5
    industry_limit: 28

workflow:
  daily_update:
    refresh_basic: true
    baostock:
      frequencies: ["d", "w", "m", "5"]
      full_update: false
      resume: true
      lookback_years: 3
      schedule:
        weekly: true
        monthly: true
    akshare:
      enabled: false
      loop_mode: false
      iterations: null
      ignore_hours: false
    tagging:
      industry: true
    indicators:
      enabled: true
      collection: "indicator_data"
      run_industry_metrics: true
      run_industry_breadth: true
      industry_metrics_lookback_days: 12
      industry_metrics_momentum: 5
      industry_limit: 28
      industry_codes: []
      breadth_indicator: "industry_breadth_ma20"
      breadth_timeframe: "1d"
      breadth_lookback_days: 60
      breadth_ma_window: 20
      jobs:
        - name: "sse_macd"
          type: "macd"
          symbol: "sh.000001"
          frequency: "d"
          params:
            fast: 12
            slow: 26
            signal: 9
```

## 数据维护流程

1. **配置校验/干跑**
   ```bash
   python daily_update.py --dry-run
   ```
   - 打印计划：将根据交易日历决定是否拉取周/月线，展示即将执行的任务。

2. **常规同步（首次/每日）**
   ```bash
   python daily_update.py
   ```
   - 自动刷新 `stock_basic`（可在配置关闭），K 线增量补齐；`index_codes` 会一并更新。
   - 周线、月线仅在交易周/月结束时触发；分钟/日线每天执行。
   - 同步完成后立即计算配置中的指标（技术指标 + 行业宽度/动量等）并写入 `indicator_collection`。

3. **实时行情（可选）**
   - 将 `workflow.daily_update.akshare.enabled` 设为 `true`，每日同步时顺带运行 Akshare 快照或循环模式。

## CLI 常用命令

```bash
# Baostock
python main.py baostock basic --refresh
python main.py baostock kline --freq d --freq w --freq 5
python main.py baostock kline --full --years 15  # 大规模回溯时使用

# Akshare
python main.py akshare once --ignore-hours --force-flush
python main.py akshare realtime --iterations 5

# Backend push
python main.py backend basic
python main.py backend kline --frequency d --start-date 2024-01-01
python main.py backend indicators
```

## 项目结构

```
.
|-- config.yaml
|-- daily_update.py           # 配置驱动的每日维护脚本（Baostock + 指标 + 可选 Akshare）
|-- main.py                   # CLI 入口
|-- requirements.txt
|-- src/
|   |-- cli.py
|   |-- data/
|   |   |-- manager_akshare.py
|   |   |-- manager_baostock.py
|   |   `-- manager_backend.py
|   |-- indicators/
|   |   |-- technical_engine.py
|   |   |-- industry_breadth.py
|   |   `-- industry_metrics.py
|   `-- utils/
|       |-- config_loader.py
|       `-- logger.py
```

## 测试与排查

- **干跑模式**：`python daily_update.py --config config.yaml --dry-run`，验证参数组合。
- **接口健康检查**：
  1. `python main.py baostock --help` / `python main.py akshare --help`
  2. `python main.py akshare once --ignore-hours --force-flush`（需网络）验证写库链路。
- **常见问题**：
  - `symbols` 为空时，确认 `baostock basic` 已落库或在配置中手动填入。
  - 盘中/盘外可借助 `--ignore-hours` 跳过交易时间判断。
（已移除 tushare 依赖，专注 baostock + akshare 数据源。）
