# Backend Push & Industry Metrics

This quick reference explains how to push MongoDB data collected by AStockDataSync into the stock middle platform backend and how to publish industry momentum/width indicators.

## Configuration Recap

```yaml
stock_middle_platform_backend:
  base_url: "http://localhost:8000/api/v1"
  login_path: "/auth/login"
  basic_path: "/stocks/basic"
  kline_path: "/stocks/kline"
  indicator_path: "/indicators/records"
  username: "please-set-me"
  password: "please-set-me"
  provider: "astock-sync"
  basic_target: "primary"      # matches /api/v1/stocks/targets
  kline_target: "primary"
  indicator_target: "primary"
  industry_metrics:
    lookback_days: 12
    momentum_period: 5
    industry_limit: 28
```

## CLI Examples

```bash
# 推送股票基础数据
python main.py backend basic --batch-size 500

# 推送日线或其他频率 K 线
python main.py backend kline --frequency d --start-date 2024-01-01

# 采集申万行业指标并推送 (industry_metrics)
python main.py backend indicators --metrics-window 12 --industry-limit 20
```

`backend indicators` 使用 Akshare 的 Shenwan 指数接口计算动量与宽度后，调用 `/api/v1/indicators/records` 写入 `indicator=industry_metrics` 记录，供 Web 前端直接展示行业动量与行业宽度图表。
