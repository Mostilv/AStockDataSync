# 后端推送与行业指标发布指南

本文汇总如何将 AStockDataSync 采集到的 MongoDB 数据推送到股票中台后端，以及如何发布行业动量/宽度指标。

## 配置回顾

```yaml
stock_middle_platform_backend:
  base_url: "http://localhost:8000/api/v1"
  login_path: "/auth/login"
  basic_path: "/stocks/basic"
  kline_path: "/stocks/kline"
  indicator_path: "/indicators/records"
  username: "请填写用户名"
  password: "请填写密码"
  provider: "astock-sync"
  basic_target: "primary"      # 对应 /api/v1/stocks/targets
  kline_target: "primary"
  indicator_target: "primary"
  industry_metrics:
    lookback_days: 12
    momentum_period: 5
    industry_limit: 28
```

## CLI 示例

```bash
# 推送股票基础数据
python main.py backend basic --batch-size 500

# 推送日线或其他频率的 K 线
python main.py backend kline --frequency d --start-date 2024-01-01

# 采集申万行业指标并推送（industry_metrics）
python main.py backend indicators --metrics-window 12 --industry-limit 20

# 计算并推送行业宽度（收盘高于 MA20 占比）
python main.py backend industry-breadth --breadth-window 30 --ma-window 20

# 一键流程（建议用于日常运行）
# 1) 增量同步基础信息 + 日线
python main.py auto sync --years 1
# 2) 推送基础/K线/行业指标/行业宽度
python main.py auto push --kline-freq d --start-date 2024-01-01
```

说明：

- `backend indicators` 调用 Akshare 的申万行业指数接口计算动量/振幅宽度，写入 `indicator=industry_metrics`。
- `backend industry-breadth` 基于本地 MongoDB 的日线行情，对每个申万一级行业计算「收盘价高于 MA20 的成分股占比」，默认回溯 30 个交易日并写入 `indicator=industry_breadth_ma20`。可配合后端 `/api/v1/indicators/records` 查询或自定义看板。
