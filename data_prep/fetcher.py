# dataprep/fetcher.py
import tushare as ts
import pandas as pd
import time

from config import TUSHARE_TOKEN

ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()

def fetch_data(ts_code: str, start_date: str, end_date: str, max_retries: int = 3) -> pd.DataFrame:
    """
    使用Tushare从指定日期区间拉取日线数据，并增加重试逻辑。
    """
    for attempt in range(max_retries):
        try:
            df = pro.daily(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                fields="ts_code,trade_date,open,high,low,close,vol,amount"
            )
            if df is not None and not df.empty:
                return df
            else:
                print(f"No data returned for {ts_code} from {start_date} to {end_date}. Attempt {attempt+1}/{max_retries}.")
        except Exception as e:
            print(f"Error fetching data: {e}. Attempt {attempt+1}/{max_retries}.")
        time.sleep(2)  # 等待2秒再重试
    print(f"Failed to fetch data for {ts_code} after {max_retries} attempts.")
    return pd.DataFrame()  # 返回空DataFrame
