import baostock as bs
import time
from pymongo import MongoClient, UpdateOne, ASCENDING
from pymongo.errors import BulkWriteError
from datetime import datetime, timedelta
from tqdm import tqdm
from utils.config_loader import load_config

START_DATE = "2014-01-01"
ADJUSTFLAG = "3"
DATE_FORMAT = "%Y-%m-%d"
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
        self.minute_15_col = self.db[baostock_cfg['minute_15']]
        self.minute_60_col = self.db[baostock_cfg['minute_60']]

        # 创建索引
        self._create_indexes()

        # 登录baostock
        lg = bs.login()
        if lg.error_code != '0':
            raise Exception(f"登录baostock失败: {lg.error_msg}")
        print("登录baostock成功")

    def _create_indexes(self):
        """创建MongoDB集合的索引以保证数据唯一性"""
        # 基本信息表对code唯一索引
        if "code_1" not in self.stock_basic_col.index_information():
            self.stock_basic_col.create_index([("code", ASCENDING)], unique=True)
        
        # 日线数据表对(code, date)唯一索引
        if "code_1_date_1" not in self.daily_col.index_information():
            self.daily_col.create_index([("code", ASCENDING), ("date", ASCENDING)], unique=True)
        
        # 分钟K线数据表对(code, datetime)唯一索引
        if "code_1_date_1_time_1" not in self.minute_15_col.index_information():
            self.minute_15_col.create_index([("code", ASCENDING), ("date", ASCENDING),("time", ASCENDING)], unique=True)
        if "code_1_date_1_time_1" not in self.minute_60_col.index_information():
            self.minute_60_col.create_index([("code", ASCENDING), ("date", ASCENDING),("time", ASCENDING)], unique=True)
        
        return

    def close(self):
        bs.logout()
        self.client.close()
        print("已登出baostock并关闭MongoDB连接")
    
    def query_stock_basic(self, refresh=False):
        """
        从baostock获取A股基本信息列表。
        :param refresh: 是否强制刷新数据。如果为True，则删除所有数据并重新插入。
        """
        expected_fields = ['code', 'code_name', 'ipoDate', 'outDate', 'type', 'status']
        rs = bs.query_stock_basic()
        stock_list = []

        # 验证字段是否匹配
        if rs.fields != expected_fields:
            raise ValueError(f"query_stock_basic func: Fields do not match the expected format. Expected: {expected_fields}, but got: {rs.fields}")
            
        while rs.next():
            row = rs.get_row_data()
            if row[5] == '1' and row[4] in ['1', '2']:  # 股票状态为上市，且 type 为 1、2、5
                stock_info = {
                    "code": row[0],
                    "code_name": row[1],
                    "ipoDate": row[2],
                    "outDate": row[3],
                    "type": row[4],
                    "status": row[5]
                }
                stock_list.append(stock_info)
        
        if refresh:
            # 删除所有数据并重新插入
            self.stock_basic_col.delete_many({})
            self.stock_basic_col.insert_many(stock_list)
        else:
            # 更新数据并删除不必要的字段
            for stock in stock_list:
                self.stock_basic_col.update_one(
                    {"code": stock["code"]},
                    {"$set": stock},
                    upsert=True
                )
        
        print(f"股票基本信息更新完成，共更新 {len(stock_list)} 条记录。")

    def query_history_k_data_plus(self, code, start_date, end_date, frequency='d'):
        """
        获取历史K线数据。
        
        参数:
        - code: 股票代码
        - start_date: 开始日期 (YYYY-MM-DD)
        - end_date: 结束日期 (YYYY-MM-DD)
        - frequency: K线周期 ('d' 日线, '5' 分钟线等)
        
        返回:
        - data_list: 包含K线数据的列表
        """
        # 使用官方提供的字段名称
        if frequency == 'd': # 日K线
            fields = "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,psTTM,pcfNcfTTM,pbMRQ,isST"
            expected_fields = ['date', 'code', 'open', 'high', 'low', 'close', 'preclose', 'volume', 'amount',
                                'adjustflag', 'turn', 'tradestatus', 'pctChg', 'peTTM', 'psTTM', 'pcfNcfTTM',
                                'pbMRQ', 'isST']
        elif frequency == '15' or frequency == '60':  # 分钟K线
            # 例如 '15m', '30m', '60m'
            fields = "date,time,code,open,high,low,close,volume,amount,adjustflag"
            expected_fields = ['date', 'time', 'code', 'open', 'high', 'low', 'close', 'volume', 'amount', 'adjustflag']
        elif frequency == 'w' or frequency == 'm': # 周、月K线
            fields = "date,time,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg"
            expected_fields = ['date', 'time', 'code', 'open', 'high', 'low', 'close', 'volume', 'amount', 'adjustflag','turn','pctChg']
        else:
            raise ValueError("Unsupported frequency. Use 'd' for daily or '1m', '5m', etc. for minute data.")
        
        for attempt in range(RETRY_LIMIT):
            try:
                rs = bs.query_history_k_data_plus(
                    code, 
                    fields, 
                    start_date, 
                    end_date,
                    frequency, 
                    adjustflag=ADJUSTFLAG
                )
                if rs.error_code != '0':
                    print(f"获取{code}k线数据失败: {rs.error_msg} (尝试 {attempt + 1}/{RETRY_LIMIT})")
                    continue

                if rs.fields != expected_fields:
                    raise ValueError(f"字段不匹配: 期望 {expected_fields}，实际 {rs.fields}")

                # 确保字段正确
                data_list = []
                while rs.next():
                    row = rs.get_row_data()
                    # 自动匹配字段，并尝试转换为 float
                    data_dict = {}
                    for key, value in zip(rs.fields, row):
                        try:
                            data_dict[key] = float(value) if value else None  # 只有非空值尝试转换
                        except ValueError:
                            data_dict[key] = value  # 非数值字段保留原始值
                    data_list.append(data_dict)

                # 按日期排序
                data_list.sort(key=lambda x: x["date"])
                return data_list
            except Exception as e:
                print(f"获取{code} {frequency}级别k线数据异常: {e} (尝试 {attempt + 1}/{RETRY_LIMIT})")
                time.sleep(1)  # 等待后重试
        print(f"获取{code} {frequency}级别k线数据失败，超出最大重试次数。")
        return []

    def query_all_stock(self,day:str="2024-10-25"):
        #### 获取某日所有证券信息 ####
        rs = bs.query_all_stock(day)
        #TODO 保存到db

    #-----------------------------------------------------------------------
    def update_stock_k_data(self, full_update=False):
        """
        更新股票 K 线数据（支持日线 & 15m & 60m）
        
        参数：
        - full_update (bool): 是否进行全量更新。默认 False（增量更新）。
        - `False` 只更新 `last_date` 之后的数据（默认）。
        - `True` 从 `START_DATE` 重新获取数据（全量更新）。
        """
        stock_list = list(self.stock_basic_col.find({}, 
            {"code": 1, "last_daily_date": 1, "last_minute_15_date": 1, "last_minute_60_date": 1}
        ))
        end_date = datetime.now().strftime(DATE_FORMAT)

        # 需要更新的数据类型（周期, 对应的MongoDB集合, 在 `stock_basic_col` 中的字段名）
        data_types = [
            # ('d', self.daily_col, "last_daily_date"),
            # ('15', self.minute_15_col, "last_minute_15_date"),
            ('60', self.minute_60_col, "last_minute_60_date")
        ]

        for freq, collection_name, field_name in data_types:
            with tqdm(total=len(stock_list), desc=f"更新 {freq} 数据", unit="stock", dynamic_ncols=True) as pbar:
                for stock in stock_list:
                    code = stock["code"]
                    last_date = stock.get(field_name)
                    
                    # **全量更新（从 `START_DATE` 开始）**
                    if full_update or not last_date:
                        start_date_str = START_DATE
                    else:
                        start_dt = datetime.strptime(last_date, DATE_FORMAT) + timedelta(days=1)
                        start_date_str = start_dt.strftime(DATE_FORMAT)

                    # **如果 `start_date_str` 超过 `end_date`，跳过**
                    if start_date_str > end_date:
                        pbar.update(1)
                        continue

                    # 查询数据
                    data_list = self.query_history_k_data_plus(code, start_date_str, end_date, freq)
                    if data_list:
                        try:
                            # 处理 `d` vs `15m/60m` 字段
                            bulk_operations = [
                                UpdateOne(
                                    {"code": data["code"], "date": data["date"], **({"time": data["time"]} if freq != 'd' else {})},
                                    {"$set": data},
                                    upsert=True
                                ) for data in data_list
                            ]

                            collection_name.bulk_write(bulk_operations, ordered=False)

                            # **更新 `stock_basic_col` 中的 `last_*_date`**
                            new_last_date = data_list[-1]["date"]
                            self.stock_basic_col.update_one({"code": code}, {"$set": {field_name: new_last_date}})

                            # **用 tqdm.write() 避免影响进度条**
                            tqdm.write(f"{code} 更新 {freq} 线数据 {len(data_list)} 条, 最新数据日期 {new_last_date}")

                        except BulkWriteError as e:
                            tqdm.write(f"批量写入数据库时出错: {e.details}")

                    pbar.update(1)
