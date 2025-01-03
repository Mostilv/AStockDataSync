import baostock as bs
import time
from pymongo import MongoClient, ASCENDING
from datetime import datetime, timedelta
from tqdm import tqdm
from utils.config_loader import load_config

START_DATE = "2014-01-01"
ADJUSTFLAG = "3"
DATE_FORMAT = "%Y-%m-%d"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
RETRY_LIMIT = 3  # 请求失败重试次数

class BaostockManager:
    def __init__(self, config_path: str = 'config.yaml'):
        """
        初始化BaostockManager对象。
        
        参数：
        config_path: 配置文件路径
        """
        # 加载配置
        self.config = load_config(config_path)
        
        # 初始化 MongoDB
        baostock_cfg = self.config['baostock']
        mongo_config = self.config['mongodb']
        self.client = MongoClient(mongo_config['uri'])
        self.db = self.client[baostock_cfg['db']]
        self.stock_basic_col = self.db[baostock_cfg['basic']]
        self.daily_col = self.db[baostock_cfg['daily']]

        # 创建索引：basic表对code唯一索引，daily表对(code, date)唯一索引
        existing_indexes_basic = self.stock_basic_col.index_information()
        if "code_1" not in existing_indexes_basic:
            self.stock_basic_col.create_index([("code", ASCENDING)], unique=True)

        existing_indexes_daily = self.daily_col.index_information()
        if "code_1_date_1" not in existing_indexes_daily:
            self.daily_col.create_index([("code", ASCENDING), ("date", ASCENDING)], unique=True)

        # 登录baostock
        lg = bs.login()
        if lg.error_code != '0':
            raise Exception(f"登录baostock失败: {lg.error_msg}")

    def close(self):
        bs.logout()
        self.client.close()

    def query_stock_basic(self):
        """
        从baostock获取A股基本信息列表。
        """
        expected_fields = ['code', 'code_name', 'ipoDate', 'outDate', 'type', 'status']
        rs = bs.query_stock_basic()
        stock_list = []

        # 验证字段是否匹配
        if rs.fields != expected_fields:
            raise ValueError(f"query_stock_basic func: Fields do not match the expected format. Expected: {expected_fields}, but got: {rs.fields}")
            
        while rs.next():
            row = rs.get_row_data()
            # 保持与baostock字段一致
            if row[4] == '1' and row[5] == '1':  #4 股票 5 上市
                stock_info = {
                    "code": row[0],
                    "code_name": row[1],
                    "ipoDate": row[2],
                    "outDate": row[3],
                    "type": row[4],
                    "status": row[5]
                }
                stock_list.append(stock_info)
                
        for stock in stock_list:
            self.stock_basic_col.update_one(
                {"code": stock["code"]},
                {"$set": stock},
                upsert=True
            )
        print(f"股票基本信息更新完成，共更新 {len(stock_list)} 条记录。")

    def query_history_k_data_plus(self, code, start_date, end_date):
        # 使用官方提供的字段名称
        expected_fields = ['date', 'code', 'open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'adjustflag', 'turn', 'tradestatus', 'pctChg', 'peTTM', 'psTTM', 'pcfNcfTTM', 'pbMRQ', 'isST']
        for attempt in range(RETRY_LIMIT):
            try:
                rs = bs.query_history_k_data_plus(
                    code, 
                    fields = "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,psTTM,pcfNcfTTM,pbMRQ,isST", 
                    start_date=start_date, 
                    end_date=end_date,
                    frequency="d", 
                    adjustflag=ADJUSTFLAG
                )
                if rs.error_code != '0':
                    print(f"获取{code}日线数据失败: {rs.error_msg} (尝试 {attempt + 1}/{RETRY_LIMIT})")
                    continue

                if rs.fields != expected_fields:
                    raise ValueError(f"字段不匹配: 期望 {expected_fields}，实际 {rs.fields}")

                data_list = []
                while rs.next():
                    row = rs.get_row_data()
                    data_list.append({
                        "date": row[0],
                        "code": row[1],
                        "open": float(row[2]) if row[2] else None,
                        "high": float(row[3]) if row[3] else None,
                        "low": float(row[4]) if row[4] else None,
                        "close": float(row[5]) if row[5] else None,
                        "preclose": float(row[6]) if row[6] else None,
                        "volume": float(row[7]) if row[7] else None,
                        "amount": float(row[8]) if row[8] else None,
                        "adjustflag": row[9],
                        "turn": float(row[10]) if row[10] else None,
                        "tradestatus": int(row[11]) if row[11] else None,
                        "pctChg": float(row[12]) if row[12] else None,
                        "peTTM": float(row[13]) if row[13] else None,
                        "psTTM": float(row[14]) if row[14] else None,
                        "pcfNcfTTM": float(row[15]) if row[15] else None,
                        "pbMRQ": float(row[16]) if row[16] else None,
                        "isST": int(row[17]) if row[17] else None
                    })
                data_list.sort(key=lambda x: x["date"])
                return data_list
            except Exception as e:
                print(f"获取{code}日线数据异常: {e} (尝试 {attempt + 1}/{RETRY_LIMIT})")
                time.sleep(1)  # 等待后重试
        print(f"获取{code}日线数据失败，超出最大重试次数。")
        return []

    def query_all_stock(self,day:str="2024-10-25"):
        #### 获取某日所有证券信息 ####
        rs = bs.query_all_stock(day)
        #TODO 保存到db

    def update_data(self):
        """批量更新所有数据，包括价格和基本面等"""
        #价格数据:
        stock_list = list(self.stock_basic_col.find({}, {"code": 1, "last_daily_date": 1}))
        end_date = datetime.now().strftime(DATE_FORMAT)

        with tqdm(total=len(stock_list), desc="更新进度") as pbar:
            for stock in stock_list:
                code = stock["code"]
                last_date = stock.get("last_daily_date")
                if last_date:
                    start_dt = datetime.strptime(last_date, DATE_FORMAT) + timedelta(days=1)
                else:
                    start_dt = datetime.strptime(START_DATE, DATE_FORMAT)
                start_date_str = start_dt.strftime(DATE_FORMAT)
                if start_date_str > end_date:
                    pbar.update(1)
                    continue
                data_list = self._get_daily_data_from_bs(code, start_date_str, end_date)
                if data_list:
                    try:
                        self.daily_col.insert_many(data_list, ordered=False)
                        new_last_date = data_list[-1]["date"]
                        self._set_last_daily_date(code, new_last_date)
                        print(f"{code} 更新日线数据 {len(data_list)} 条")
                    except Exception as e:
                        print(f"写入数据库时出错: {e}")
                pbar.update(1)
                
        #基本面

