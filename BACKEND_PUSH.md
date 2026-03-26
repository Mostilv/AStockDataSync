# 后端推送说明

本项目通过 `src/utils/backend_client.py` 与后端通信。

## 当前会用到的接口

- `POST /auth/login`
- `POST /stocks/basic`
- `POST /stocks/kline`
- `POST /indicators/records`
- `POST /data/market/indices`
- `POST /data/limit_up/pool`
- `POST /integrity/check`

## 配置示例

```yaml
stock_middle_platform_backend:
  enabled: true
  base_url: "http://localhost:8000"
  api_prefix: "/api/v1"
  username: "please-set-me"
  password: "please-set-me"
  target: "primary"
```

## 说明

- `BackendClient` 会先登录，再携带 Bearer Token 调用后端
- 当前 `run_sync.py` 主要使用市场指数、涨停池和指标推送能力
- 如果后端接口路径调整，需要同步修改 `backend_client.py`
