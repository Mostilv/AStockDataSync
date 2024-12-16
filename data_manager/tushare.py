import time
from tqdm import tqdm
from pymongo import MongoClient, ASCENDING
import tushare as ts
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from utils.config_loader import load_config

class TushareManager:
    #TODO 1.记录每支股票最新数据的日期，用于获取数据时不重复获取
    #     2.没有数据的股票最新日期为开始日期
    def __init__(self,config_path: str = 'config.yaml'):
        """
        初始化TushareManager对象。
        
        参数：
        config_path: 配置文件路径
        """
        # 加载配置
        self.config = load_config(config_path)
        
        # 初始化 Tushare
        tushare_config = self.config['tushare']
        self.token = tushare_config['token']
        ts.set_token(self.token)
        self.pro = ts.pro_api()

        # 初始化 MongoDB
        mongo_config = self.config['mongodb']
        self.client = MongoClient(mongo_config['uri'])
        self.db = self.client[tushare_config['db']]
        self.collection_basic = self.db[tushare_config['collection_basic']]
        self.collection_daily = self.db[tushare_config['collection_daily']]
        
        # 为 basic 和 daily 集合创建索引
        existing_indexes_basic = self.collection_basic.index_information()
        if "ts_code_1" not in existing_indexes_basic:
            self.collection_basic.create_index([("ts_code", ASCENDING)], unique=True)
        existing_indexes_daily = self.collection_daily.index_information()
        if "ts_code_1_trade_date_1" not in existing_indexes_daily:
            self.collection_daily.create_index([("ts_code", ASCENDING), ("trade_date", ASCENDING)], unique=True)

        # 速率控制参数
        self.max_requests = 500  # 65秒内最多500次请求
        self.request_count = 0
        self.start_time = time.time()
        self.lock = threading.Lock()  # 用于保护 request_count 和速率控制

    # 获取股票列表并存储在basic集合
    def fetch_stock_basic(self):
        stock_list = self.pro.stock_basic(**{
            "ts_code": "",
            "name": "",
            "exchange": "",
            "market": "",
            "is_hs": "",
            "list_status": "",
            "limit": "",
            "offset": ""
        }, fields=[
            "ts_code",
            "symbol",
            "name",
            "area",
            "industry",
            "cnspell",
            "market",
            "list_date",
            "act_name",
            "act_ent_type"
        ])
        records = stock_list.to_dict('records')
        
        for record in records:
            self.collection_basic.replace_one(
                {'ts_code': record['ts_code']}, 
                record, 
                upsert=True
            )
        print("func:'fetch_stock_basic' finished")
        return

    def fetch_one_day_data(self,
                        trade_date: str = time.strftime('%Y%m%d', time.localtime()),
                        max_retries:int = 3
                        ):
        for attempt in range(max_retries):
            try:
                df = self.pro.daily(
                    trade_date = trade_date,
                    fields = ["ts_code",
                            "trade_date",
                            "open",
                            "high",
                            "low",
                            "close",
                            "pre_close",
                            "change",
                            "pct_chg",
                            "vol",
                            "amount"]
                )

                if df is not None and not df.empty:
                    return df
                else:
                    print(f"No data for trade_date. Attempt {attempt+1}/{max_retries}.")

            except Exception as e:
                print(f"Error fetching data from day {trade_date}: {e}. Attempt {attempt+1}/{max_retries}.")
            time.sleep(2)  # 等待2秒再重试
        print(f"Failed to fetch data from day {trade_date} after {max_retries} attempts.")
        return pd.DataFrame()

    # 获取日线数据（单支股票）
    def fetch_daily_data(self, 
                        ts_code: str, 
                        start_date: str,
                        end_date: str,
                        max_retries:int = 3) -> pd.DataFrame:
        for attempt in range(max_retries):
            try:
                df = self.pro.daily(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date,
                    fields=["ts_code",
                            "trade_date",
                            "open",
                            "high",
                            "low",
                            "close",
                            "pre_close",
                            "change",
                            "pct_chg",
                            "vol",
                            "amount"]
                )

                if df is not None and not df.empty:
                    return df
                else:
                    print(f"No data for {ts_code} from {start_date} to {end_date}. Attempt {attempt+1}/{max_retries}.")

            except Exception as e:
                print(f"Error fetching data for {ts_code}: {e}. Attempt {attempt+1}/{max_retries}.")
            time.sleep(2)  # 等待2秒再重试
        print(f"Failed to fetch data for {ts_code} after {max_retries} attempts.")
        return pd.DataFrame()

    # 将获取的日线数据保存到MongoDB
    def save_to_mongo(self, df: pd.DataFrame, ts_code: str = None):
        if df is None or df.empty:
            print(f"No data to save. {ts_code} ")
            return
        records = df.to_dict('records')
        for r in records:
            query = {"ts_code": r["ts_code"], "trade_date": r["trade_date"]}
            self.collection_daily.update_one(query, {"$set": r}, upsert=True)
        if not ts_code:
            print("func save_to_mongo finished")

    def _fetch_and_save(self, ts_code: str, start_date: str, end_date: str):
        """
        多线程调用的工作函数：
        在这个函数中进行速率控制，获取日线数据，并保存至MongoDB。
        """
        # 速率控制
        with self.lock:
            # 如果已经达到500次请求，则需要计算时间窗是否小于65秒
            if self.request_count >= self.max_requests:
                elapsed_time = time.time() - self.start_time
                if elapsed_time < 65:
                    sleep_time = 65 - elapsed_time
                    print(f"\nRate limit reached, sleeping for {sleep_time:.2f} seconds...")
                    time.sleep(sleep_time)
                # 重置计数器和时间
                self.request_count = 0
                self.start_time = time.time()

            self.request_count += 1
        
        # 获取数据
        df = self.fetch_daily_data(ts_code, start_date, end_date)
        # 保存数据
        self.save_to_mongo(df, ts_code)
        return ts_code

    # 使用多线程获取所有股票的数据
    def fetch_all_daily_data(self,
                            start_date: str = "20150101",
                            end_date: str = time.strftime('%Y%m%d', time.localtime()),
                            max_threads: int = 30):
        """
        执行完整的数据获取、存储流程，多线程版本。
        tushare接口限制了每65秒内最多调取500次。
        
        参数：
        start_date: 开始日期 "YYYYMMDD"
        end_date: 结束日期 "YYYYMMDD"
        max_threads: 并行线程数量
        """
        # 从stock_basic集合中获取股票列表
        ts_basic = self.collection_basic.find({}, {"ts_code": 1})
        ts_code_list = [stock["ts_code"] for stock in ts_basic]

        # 使用线程池
        from concurrent.futures import ThreadPoolExecutor, as_completed

        with ThreadPoolExecutor(max_workers=max_threads) as executor, \
                tqdm(total=len(ts_code_list), desc="Fetching daily data", unit="stock") as pbar:
            futures = []
            for ts_code in ts_code_list:
                future = executor.submit(self._fetch_and_save, ts_code, start_date, end_date)
                futures.append(future)

            for f in as_completed(futures):
                ts_code = f.result()
                pbar.update(1)
                pbar.set_postfix({"last_finished": ts_code})

        print("func: 'fetch_all_daily_data' finished")
