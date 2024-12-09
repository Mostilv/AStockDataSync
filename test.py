import time
import tushare as ts

# 设置Tushare Token (请替换为你的实际token)
ts.set_token('b8a293cd8e00b775f3b410f1140d0402ea9fc103b0eac5a408e62cee')
pro = ts.pro_api()

# 目标股票代码（注意沪市后缀为.SH，深市后缀为.SZ）
ts_code = "600435.SH"
# 指定时间区间（如获取2020年全年的数据）
start_date = ""
end_date = ""

# 重试次数和等待时间设置
max_retries = 3
retry_wait = 5  # 秒

for attempt in range(max_retries):
    try:
        df = pro.daily(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            fields=[
                "ts_code",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "pre_close",
                "change",
                "pct_chg",
                "vol",
                "amount"
            ]
        )
        # 如果请求成功，打印数据并结束循环
        print(df)
        break
    except Exception as e:
        print(f"Request failed on attempt {attempt+1}/{max_retries}: {e}")
        # 如果还没有到最大重试次数则等待后继续重试
        if attempt < max_retries - 1:
            print(f"Waiting {retry_wait} seconds before retry...")
            time.sleep(retry_wait)
        else:
            print("All retry attempts failed. Please check your network or token.")
