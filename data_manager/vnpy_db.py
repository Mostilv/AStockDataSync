from datetime import datetime
import pytz
from pymongo import MongoClient
from tqdm import tqdm
from vnpy.trader.object import BarData
from vnpy.trader.constant import Exchange, Interval
from vnpy_mongodb import Database
from concurrent.futures import ThreadPoolExecutor, as_completed
import gc
from utils.config_loader import load_config

LOCAL_TZ = pytz.timezone("Asia/Shanghai")

class VnpyDBManager:
    def __init__(self, config_path: str = 'config.yaml'):
        """初始化数据库管理器"""
        # 加载配置文件
        
        self.config = load_config(config_path)

        # 初始化 vnpy_mongodb 数据库
        self.db = Database()

        # baostock 数据配置
        mongo_cfg = self.config['mongodb']
        baostock_cfg = self.config['baostock']
        self.client = MongoClient(mongo_cfg['uri'])
        self.baostock_db = self.client[baostock_cfg["db"]]
        self.baostock_stock_basic_col = self.baostock_db[baostock_cfg["basic"]]
        self.baostock_daily_col = self.baostock_db[baostock_cfg["daily"]]

    @staticmethod
    def _convert_symbol_format(code):
        """将 baostock 代码转换为 vn.py 格式"""
        prefix, symbol = code.split('.')
        exchange = Exchange.SZSE if prefix == "sz" else Exchange.SSE
        return symbol, exchange

    @staticmethod
    def _convert_daily_bar(symbol, exchange, doc):
        """将 baostock 的日线数据转换为 vn.py 的 BarData 格式"""
        date = datetime.strptime(doc["date"], "%Y-%m-%d")
        local_dt = LOCAL_TZ.localize(date)
        utc_dt = local_dt.astimezone(pytz.UTC)

        return BarData(
            symbol=symbol,
            exchange=exchange,
            interval=Interval.DAILY,
            datetime=utc_dt,
            open_price=doc["open"],
            high_price=doc["high"],
            low_price=doc["low"],
            close_price=doc["close"],
            volume=doc["volume"],
            open_interest=0,  # baostock 没有此字段，默认为 0
            gateway_name="DB",
        )

    def _process_documents(self, docs):
        """批量处理文档的转换和保存"""
        bars = []
        for doc in docs:
            if doc.get("tradestatus", 1) == 0:
                continue

            symbol, exchange = self._convert_symbol_format(doc["code"])
            if not symbol or not exchange:
                continue

            bar = self._convert_daily_bar(symbol, exchange, doc)
            bars.append(bar)

        if bars:
            self.db.save_bar_data(bars)
        # 显式释放内存
        del bars
        gc.collect()

    def convert_all_baostock_to_vnpy(self, max_workers=10, batch_size=10000):
        """将所有 baostock 日线数据转换为 vn.py 格式并保存，支持多线程和批量处理"""
        cursor = self.baostock_daily_col.find(batch_size=batch_size)
        total = self.baostock_daily_col.count_documents({})

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            with tqdm(total=total, desc="转换baostock数据") as pbar:
                futures = []
                batch = []

                for doc in cursor:
                    batch.append(doc)
                    if len(batch) >= batch_size:
                        futures.append(executor.submit(self._process_documents, batch))
                        batch = []
                        pbar.update(batch_size)

                        # 每次提交任务后，清理无用的任务和数据
                        for future in as_completed(futures):
                            future.result()  # 确保任务完成
                        futures.clear()

                # 提交最后一批
                if batch:
                    futures.append(executor.submit(self._process_documents, batch))
                    pbar.update(len(batch))

                # 确保所有任务完成
                for future in as_completed(futures):
                    future.result()

        # 显式清理所有未释放的内存
        gc.collect()
        print("所有 baostock 数据已成功转换并保存到 vn.py 数据库！")

    def close(self):
        """关闭数据库连接"""
        if hasattr(self.db, 'client') and self.db.client:
            self.db.client.close()  # 显式关闭 pymongo.MongoClient
        print("数据库连接已关闭")

