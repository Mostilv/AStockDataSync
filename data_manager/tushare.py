#tushare 相关的数据获取及对应mongo数据库管理
import tushare as ts
import pandas as pd
import time

from config import MONGODB_URI,TUSHARE_TOKEN,TUSHARE_DB,COLLECTION_DAILY

import datetime
import pandas as pd
import tushare as ts
from pymongo import MongoClient, ASCENDING

class TushareManager:
    def __init__(self, 
                tushare_token: str = TUSHARE_TOKEN, 
                mongo_uri: str = MONGODB_URI, 
                db_name: str = TUSHARE_DB,
                collection_name: str = COLLECTION_DAILY):
        """
        初始化TushareManager对象。
        
        参数：
        tushare_token: 你的Tushare接口token
        mongo_uri: MongoDB连接字符串
        db_name: 数据库名
        collection_name: 集合名
        """
        #tushare
        self.token = tushare_token
        ts.set_token(tushare_token)
        self.pro = ts.pro_api()
        #数据库
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        #索引
        existing_indexes = self.collection.index_information()
        if "ts_code_1_trade_date_1" not in existing_indexes:
            # 为(symbol, trade_date)创建唯一索引，有助于加快查询和防止重复插入
            self.collection.create_index([("ts_code", ASCENDING), ("trade_date", ASCENDING)], unique=True)

    #获取股票列表
    
    #获取日线数据
    def fetch_daily_data(self, 
                        ts_code: str, 
                        start_date: str,
                        end_date: str,
                        max_retries:int = 3) -> pd.DataFrame:
        """
        从Tushare获取一只股票(ts_code)在指定日期范围的日线行情数据。
        
        参数：
        ts_code: 股票代码（带交易所后缀，如"000001.SZ"）
        start_date: 开始日期，格式"YYYYMMDD"
        end_date: 结束日期，格式"YYYYMMDD"
        max_retries: 请求失败时的最大重试次数
        
        返回：
        pd.DataFrame, 包含日线数据
        """
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
                    print(f"No data returned for {ts_code} from {start_date} to {end_date}. Attempt {attempt+1}/{max_retries}.")

            except Exception as e:
                print(f"Error fetching data: {e}. Attempt {attempt+1}/{max_retries}.")
            time.sleep(2)  # 等待2秒再重试
        print(f"Failed to fetch data for {ts_code} after {max_retries} attempts.")
        return pd.DataFrame()  # 返回空DataFrame

    #保存到数据库
    def save_to_mongo(self, df: pd.DataFrame):
        """
        将DataFrame插入或更新至MongoDB中。
        利用upsert方式在(symbol, trade_date)有冲突时更新数据。
        
        参数：
        df: 需要保存的数据DataFrame
        """
        if df is None or df.empty:
            print("No data to save.")
            return

        # 将每一行转换为dict并更新或插入MongoDB
        records = df.to_dict('records')
        for r in records:
            query = {"ts_code": r["ts_code"], "trade_date": r["trade_date"]}
            self.collection.update_one(query, {"$set": r}, upsert=True)

    #获取所有股票的数据直到查询次数上限
    def fetch_all_daily_data(self,
                            start_date: str = 20150101,
                            end_date: str = time.strftime('%Y%m%d', time.localtime())
                            ):
        """
        执行完整的数据获取、存储流程,获取2015年至今的所有股票数据
        每分钟内最多调取500次,每次6000条数据
        
        参数：
        ts_code: 股票代码，如"000001.SZ"
        start_date: 开始日期 "YYYYMMDD"
        end_date: 结束日期 "YYYYMMDD"
        """
        #获取股票列表
        #循环获取数据，每1分5秒执行一次循环,每次调取500次,直到获取所有数据
        print("Data saved to MongoDB.")


if __name__ == "__main__":
    # 使用示例
    manager = TushareManager()

    manager.run(ts_code="000001.SZ", start_date="20200101", end_date="20201231")
