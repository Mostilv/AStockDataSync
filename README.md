# mosQuant 数据维护指南

mosQuant 面向 MongoDB 的 A 股行情/财务数据维护工具。历史数据由 **baostock** 负责，盘中/收盘补齐由 **akshare** 提供；`tushare` 相关代码仍保留（未来可继续拓展），但当前默认不启用。项目已重构为 `mosquant/` 包，可通过 CLI 或脚本方式统一操作。

## 核心功能

- **BaostockManager (`mosquant.data.manager_baostock`)**
  - 统一管理登录/登出，按 `config.yaml` 建立 MongoDB 连接与集合索引。
  - `sync_k_data` 覆盖日/周/月/15m/60m。15/60 分钟线固定自 2019-01-02 起取数，日/周/月默认回溯 10 年（可通过 CLI 或配置覆盖）。
  - `sync_finance_data` 自动拉取 baostock 提供的季频资产负债/利润/现金流/杜邦指标，并维持最近 10 年窗口。
  - 新增 `run_integrity_check`，每周末自动重拉近窗数据，补齐可能的缺口。
- **AkshareRealtimeManager (`mosquant.data.manager_akshare`)**
  - 基于 `stock_zh_a_spot_em` 获取实时快照，并可按配置股票集合合成 15m/60m K 线。
  - 支持单次补齐或循环模式，可在盘后强制落库当前 bar。
- **每日维护脚本 `daily_update.py`**
  - 串联 baostock K 线、季频财务、周末完整性校验与 akshare 快照。
  - 提供 `--dry-run`、`--skip-finance`、`--force-integrity`、`--skip-akshare` 等开关，方便排查与定制。
- **CLI `python main.py`**
  - `baostock` 子命令覆盖 `basic`（基础信息）、`kline`（多周期 K 线）、`finance`（季频财务）。
  - `akshare` 子命令支持 `once`、`realtime`；`tushare` 子命令保留以便未来扩展。

## 安装与配置

```bash
pip install -r requirements.txt
```

`config.yaml` 示例（可按需调整库名/集合/窗口）：

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
  history_years: 10             # 日/周/月默认回溯年限
  finance_history_years: 10     # 季频财务回溯年限
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
  symbols: []                 # 留空时自动读取 stock_basic 中的 code
  timeframes: ["15m", "60m"]
  sleep_seconds: 5
```

## 数据维护流程

1. **首次运行**
   ```bash
   python daily_update.py --refresh-basic --full
   ```
   - `sync_k_data` 会按配置周期自动建库：日/周/月拉取最近 10 年，15m/60m 自 2019-01-02 起拉取。
   - `sync_finance_data` 覆盖最近 10 年季度财务；完成后会将 `last_*` 标记写回 `stock_basic`。

2. **日常维护（工作日）**
   ```bash
   python daily_update.py
   ```
   - Baostock 仅增量更新：根据每个标的的 `last_*` 日期补齐最新可得数据。
   - Akshare 默认拉一次快照并强制刷新当前 bar，可在盘后补齐当日缺失。

3. **周末校验**
   - `daily_update.py` 会在周末自动触发 `run_integrity_check`，按照配置窗口重拉近段数据并查漏补缺。
   - 可用 `--force-integrity` 在任意日期手动触发，或用 `--skip-integrity-check` 跳过。

4. **干跑验证**
   ```bash
   python daily_update.py --dry-run
   ```
   - 仅打印计划，不会真正登录 baostock/akshare。

## CLI 常用命令

```bash
# Baostock
python main.py baostock basic --refresh
python main.py baostock kline --freq d --freq w --freq 60
python main.py baostock kline --full --years 15            # 指定自定义窗口
python main.py baostock finance --years 8                  # 季频财务回补最近 8 年

# Akshare
python main.py akshare once --ignore-hours --force-flush
python main.py akshare realtime --iterations 5

# Tushare（如获得 token 可启用）
python main.py tushare basic
```

## 项目结构

```
.
|-- config.yaml
|-- daily_update.py           # 每日维护脚本（Baostock + integrity + Akshare）
|-- main.py                   # CLI 入口
|-- requirements.txt
|-- mosquant/
|   |-- cli.py
|   |-- data/
|   |   |-- manager_akshare.py
|   |   |-- manager_baostock.py
|   |   `-- manager_tushare.py
|   `-- utils/
|       |-- config_loader.py
|       |-- helper.py
|       `-- logger.py
`-- test*.py
```

## 测试与排查

- **干跑模式**：`python daily_update.py --dry-run`，快速验证参数组合。
- **接口健康检查**：
  1. `python main.py baostock --help` / `python main.py akshare --help` 查看 CLI 配置。
  2. `python main.py akshare once --ignore-hours --force-flush`（需网络）验证 akshare 接口与 Mongo 写入。
- **常见问题**：
  - `symbols` 为空时，请确认已通过 `baostock basic` 拉取股票列表，或在配置中手动填写。
  - 盘中/盘外执行可利用 `--ignore-hours` 跳过交易时间判断。
  - 若需要启用 tushare，请在 `config.yaml` 中补充 `tushare.token` 并在 CLI 中使用相应子命令。
