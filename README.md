# AStockDataSync 数据维护指南

AStockDataSync 是一套面向 MongoDB 的 A 股行情与财务数据维护工具，定位为“统一从公开接口获取数据并落地本地库”的入口。历史数据由 **baostock** 负责，盘中/收盘补齐由 **akshare** 提供；`tushare` 代码保留以备后续扩展，但默认不开启。项目重构为 `src/` 包，可通过 CLI 或脚本灵活调用。

## 核心功能

- **BaostockManager (`src.data.manager_baostock`)**
  - 统一登录/登出，按照 `config.yaml` 建立 MongoDB 连接并创建索引。
  - `sync_k_data` 覆盖日/周/月/15m/60m，多数频率默认回溯 10 年；分钟级从 2019-01-02 固定起始。
  - `sync_finance_data` 自动拉取季频资产负债、利润、现金流、杜邦指标，维持至少 10 年窗口。
  - `run_integrity_check` 每周重新拉取近窗数据，修补缺口。
  - 内置 Baostock 调用计数，默认每日 15 万次上限，超限立即停止以保护账号。
- **AkshareRealtimeManager (`src.data.manager_akshare`)**
  - 基于 `stock_zh_a_spot_em` 获取实时快照，并按配置组合生成 15m/60m K 线。
  - 支持单次补齐或循环模式，可在收盘后强制落库当前 bar。
- **每日维护脚本 `daily_update.py`**
  - 串联 Baostock K 线、季频财务、周末完整性校验与 Akshare 快照。
  - 提供 `--dry-run`、`--skip-finance`、`--force-integrity`、`--skip-akshare` 等开关。
- **CLI `python main.py`**
  - `baostock` 子命令覆盖 `basic`、`kline`、`finance`。
  - `akshare` 子命令支持 `once`、`realtime`；`tushare` 子命令保留备用。

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
  minute_15: "minute_15_adjusted"
  minute_60: "minute_60_adjusted"
  finance_quarterly: "finance_quarterly"
  history_years: 10
  finance_history_years: 10
  daily_call_limit: 150000
  minute_start_date: "2019-01-02"
  frequencies: ["d", "w", "m", "15", "60"]
  integrity_windows:
    d: 30
    w: 400
    m: 1500
    "15": 15
    "60": 45

akshare:
  db: "akshare_realtime"
  kline: "kline"
  daily: "daily"
  symbols: []            # 留空时自动读取 stock_basic 中的 code
  timeframes: ["15m", "60m"]
  sleep_seconds: 5
```

## 数据维护流程

1. **首次运行**
   ```bash
   python daily_update.py --refresh-basic --full
   ```
   - `sync_k_data` 自动建库：日/周/月回溯 10 年，15m/60m 自 2019-01-02 开始。
   - `sync_finance_data` 拉取近 10 年季度财务，并把 `last_*` 标记写回 `stock_basic`。
   - 初始化若中断，可通过 `--resume` 读取现有进度继续。

2. **工作日日常维护**
   ```bash
   python daily_update.py
   ```
   - Baostock 仅增量更新，按各标的 `last_*` 日期补齐。
   - Akshare 默认拉取一次快照并强制刷新当前 bar。

3. **周末完整性校验**
   - `daily_update.py` 在周末自动触发 `run_integrity_check`，按配置窗口重新拉取并修补数据。
   - 支持 `--force-integrity` 随时触发或 `--skip-integrity-check` 跳过。

4. **干跑模式**
   ```bash
   python daily_update.py --dry-run
   ```
   - 仅打印计划，不会真正登录接口。

## CLI 常用命令

```bash
# Baostock
python main.py baostock basic --refresh
python main.py baostock kline --freq d --freq w --freq 60
python main.py baostock kline --full --years 15
python main.py baostock kline --full --resume
python main.py baostock finance --years 8

# Akshare
python main.py akshare once --ignore-hours --force-flush
python main.py akshare realtime --iterations 5

# Tushare（取得 token 后再启用）
python main.py tushare basic
```

## 项目结构

```
.
|-- config.yaml
|-- daily_update.py           # 每日维护脚本（Baostock + Integrity + Akshare）
|-- main.py                   # CLI 入口
|-- requirements.txt
|-- src/
|   |-- cli.py
|   |-- data/
|   |   |-- manager_akshare.py
|   |   |-- manager_baostock.py
|   |   `-- manager_tushare.py
|   `-- utils/
|       |-- config_loader.py
|       `-- logger.py
```

## 测试与排查

- **干跑模式**：`python daily_update.py --dry-run`，验证参数组合。
- **接口健康检查**：
  1. `python main.py baostock --help` / `python main.py akshare --help`
  2. `python main.py akshare once --ignore-hours --force-flush`（需网络）验证写库链路。
- **常见问题**：
  - `symbols` 为空时，确认 `baostock basic` 已落库或在配置中手动填入。
  - 盘中/盘外可借助 `--ignore-hours` 跳过交易时间判断。
  - 若需启用 `tushare`，请在 `config.yaml` 中补充 token 并使用相应子命令。
