# vnpy_db.py

from pymongo import MongoClient
from datetime import datetime
import pytz
from utils.config_loader import load_config

LOCAL_TZ = pytz.timezone("Asia/Shanghai")


class VnpyDBManager:
    def __init__(self, config_path: str = 'config.yaml'):
        # 加载配置
        self.config = load_config(config_path)
        mongo_cfg = self.config['mongodb']
        baostock_cfg = self.config['baostock']
        tushare_cfg = self.config['tushare']

        #数据库
        self.client = MongoClient(mongo_cfg['uri'])
        self.baostock_db = self.client[baostock_cfg["db"]]
        self.stock_basic_col = self.baostock_db[baostock_cfg["basic"]]
        self.daily_col = self.baostock_db[baostock_cfg["daily"]]

    def close(self):
        self.client.close()

    @staticmethod
    def _convert_symbol_format(bs_symbol):
        prefix, code = bs_symbol.split('.')
        if prefix == 'sh':
            exchange = "SSE"
        elif prefix == 'sz':
            exchange = "SZSE"
        else:
            return None, None
        vnpy_symbol = f"{code}.{exchange}"
        return vnpy_symbol, exchange

    def _convert_daily_bar(self, symbol, exchange, doc):
        dt = datetime.strptime(doc["date"], "%Y-%m-%d")
        local_dt = LOCAL_TZ.localize(dt)
        utc_dt = local_dt.astimezone(pytz.UTC)
        return {
            "symbol": symbol,
            "exchange": exchange,
            "interval": "1d",
            "datetime": utc_dt.isoformat(),
            "open_price": doc["open"],
            "high_price": doc["high"],
            "low_price": doc["low"],
            "close_price": doc["close"],
            "volume": doc["volume"],
            "gateway_name": "DB"
        }

    def convert_all_daily_to_vnpy(self):
        """将所有股票的日线数据转换为vn.py格式"""
        symbols = self.stock_basic_col.distinct("symbol")
        for bs_symbol in symbols:
            vnpy_symbol, exchange = self._convert_symbol_format(bs_symbol)
            if not vnpy_symbol:
                continue

            cursor = self.daily_col.find({"symbol": bs_symbol}).sort("date", 1)
            docs_to_insert = []
            for d in cursor:
                if d["tradestatus"] == 0:
                    continue
                bar = self._convert_daily_bar(vnpy_symbol, exchange, d)
                docs_to_insert.append(bar)
            if docs_to_insert:
                self.vnpy_col.insert_many(docs_to_insert)
                print(f"{bs_symbol} 日线数据转换 {len(docs_to_insert)} 条")
