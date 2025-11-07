# mosQuant 数据维护指南

面向 MongoDB 的 A 股行情维护工具，历史数据由 **baostock** 负责，实时补齐由 **akshare** 提供；tushare 相关代码依旧保留，但在当前环境中默认不触发（没有 tushare Pro 令牌时可以忽略）。项目重构为 `mosquant/` 包，可通过 CLI 或独立脚本统一操作。

## 核心功能
- **BaostockManager (`mosquant.data.manager_baostock`)**
  - 登录/登出生命周期管理，统一读取 `config.yaml`。
  - `sync_k_data` 会先检查 MongoDB 是否已有该标的的数据：  
    - 无记录 → 按配置的 `history_years`（默认 10 年）回溯，确保首批数据覆盖最近 N 年。  
    - 已有记录 → 依据每个标的的最新日期增量拉取，落库后写回 `last_*_date` 标记，保证逐标的完整性。
  - 支持日线、15 分钟、60 分钟等多个周期（可在配置或命令行指定）。
- **AkshareRealtimeManager (`mosquant.data.manager_akshare`)**
  - 基于 `stock_zh_a_spot_em` 获取实时快照，并可按配置的股票列表（默认为 `stock_basic` 中所有代码）合成 15m/60m K 线。
  - 每次运行可选择“只拉一次并强制刷新当前 bar”或“持续轮询”，适合在每日收市后补齐当日数据。
- **每日手动脚本 `daily_update.py`**
  - 一条命令串起 Baostock 历史维护与 Akshare 实时补齐，满足“每天手动执行一次”的需求。
  - 提供 `--dry-run`、`--skip-akshare` 等开关，方便测试/调优。
- **CLI `python main.py`**
  - `baostock` 子命令支持 `basic`、`kline`。`kline` 已加入 `--freq/--years` 控制。
  - 新增 `akshare` 子命令，可在 CLI 里直接触发一次性/循环式实时采集。
  - `tushare` 子命令仍在（兼容未来需求），但默认不执行。

## 安装与配置
```bash
pip install -r requirements.txt
```

`config.yaml` 示例（可按需修改库名/集合/年限等）：

```yaml
mongodb:
  uri: "mongodb://localhost:27017/"

baostock:
  db: "baostock"
  basic: "stock_basic"
  daily: "daily_adjusted"
  minute_15: "minute_15_adjusted"
  minute_60: "minute_60_adjusted"
  history_years: 10            # 仓库无数据时回溯 N 年
  frequencies: ["d", "60"]     # 默认同步的周期，可改为 ["d", "15", "60"]

akshare:
  db: "akshare_realtime"
  kline: "kline"
  daily: "daily"
  symbols: []                  # 留空时自动读取 stock_basic 中的 code
  timeframes: ["15m", "60m"]
  sleep_seconds: 5
```

## 数据获取流程
1. **首次执行**  
   ```bash
   python daily_update.py --refresh-basic
   ```  
   - 若 Mongo 中没有任何 K 线记录，将自动从 `history_years` 指定的窗口回溯（默认 10 年）。  
   - 同步结束后会写入 `last_daily_date` 等标记，为后续增量做准备。
2. **日常维护**  
   ```bash
   python daily_update.py
   ```  
   - Baostock 只更新上次日期之后的增量。  
   - Akshare 默认在当前时间窗口抓一次实时快照，并强制写入当天 bar，方便在盘后补齐。
3. **需要测试而不触发外部接口时**  
   ```bash
   python daily_update.py --dry-run
   ```

## CLI 常用命令
```bash
# 1) Baostock
python main.py baostock basic --refresh                 # 刷新股票列表
python main.py baostock kline --freq d --freq 60        # 同步日线+60分钟，增量模式
python main.py baostock kline --full --years 15         # 直接回补 15 年，忽略历史标记

# 2) Akshare
python main.py akshare once --ignore-hours --force-flush   # 在任意时间拉一次并落库
python main.py akshare realtime --iterations 5             # 调试时循环 5 次后退出

# 3) Tushare（能力保留，如未来拿到 token 可直接使用）
python main.py tushare basic
```

## 项目结构
```
.
|-- config.yaml
|-- daily_update.py           # 每日维护脚本（串联 Baostock + Akshare）
|-- main.py                   # CLI 入口
|-- requirements.txt
|-- mosquant/
|   |-- __init__.py
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

## 测试与排障
- **干跑验证**：`python daily_update.py --dry-run` 会打印计划步骤，不会登录 baostock 或请求 akshare，可安全地在无网络/无账号场景下检查参数。
- **接口健康检查**：
  1. `python main.py baostock --help` / `python main.py akshare --help` 确认 CLI 参数无误。
  2. `python main.py akshare once --ignore-hours --force-flush`（需要网络）可验证 akshare 接口和 Mongo 写入是否正常。
- **常见问题**：
  - 如果 `symbols` 为空，请确认 `stock_basic` 已拉取或在 `config.yaml` 中手动填入需要的代码列表。
  - 当天尚未开市、或在休市时间执行，可以加上 `--ignore-hours` 以跳过交易时间判断。
  - Tushare 未配置 token 时不要运行相关命令；需要时在 `config.yaml` 增加 `tushare` 节并填入 `token`。
