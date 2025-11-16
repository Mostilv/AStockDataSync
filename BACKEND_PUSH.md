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
```

`backend indicators` 会调用 Akshare 的申万行业接口计算动量与宽度，并通过 `/api/v1/indicators/records` 写入 `indicator=industry_metrics` 的记录，供 Web 前端直接渲染行业动量与行业宽度图表。
