import time
from datetime import datetime, timedelta
import akshare as ak
import pandas as pd
import pytz
from pymongo import MongoClient, errors, ASCENDING, DESCENDING
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("realtime_kline.log"),
        logging.StreamHandler()
    ]
)

# MongoDB 配置
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "akshare_realtime"
MONGO_COLLECTION_KLINE = "kline"
MONGO_COLLECTION_DAILY = "daily"

# 交易时间配置（北京时间）
TRADING_DAYS = [0, 1, 2, 3, 4]  # 周一到周五
TRADING_PERIODS = [
    {"start": "09:30", "end": "11:30"},
    {"start": "13:00", "end": "15:00"}
]

# 要跟踪的股票列表
SYMBOL_LIST = ["000001", "000002", "600519"]  # 例如：平安银行(000001)、万科A(000002)、贵州茅台(600519)

# 抓取间隔（秒）
SLEEP_SEC = 5.0

# 时区设置
BEIJING_TZ = pytz.timezone('Asia/Shanghai')


class TimeframeAggregator:
    """
    用于合成特定时间周期(如 15m, 60m)的 K 线聚合器
    """
    def __init__(self, timeframe: str, symbol: str, db_collection):
        """
        :param timeframe: '15m' 或 '60m'
        :param symbol: 6 位股票代码，如 '000001'
        :param db_collection: MongoDB 中对应的集合
        """
        self.timeframe = timeframe
        self.symbol = symbol
        self.db_collection = db_collection

        # 当前正在构建的 Bar
        self.current_bar_start = None
        self.open_price = None
        self.high_price = None
        self.low_price = None
        self.close_price = None
        self.volume = 0

    def _get_bar_start_15m(self, dt: datetime) -> datetime:
        """ 获取 15 分钟 Bar 的起始时间（xx:00、xx:15、xx:30、xx:45） """
        total_minutes = dt.hour * 60 + dt.minute
        bar_index = total_minutes // 15
        bar_start_minute = bar_index * 15
        hour = bar_start_minute // 60
        minute = bar_start_minute % 60
        return dt.replace(hour=hour, minute=minute, second=0, microsecond=0)

    def _get_bar_start_60m(self, dt: datetime) -> datetime:
        """ 获取 60 分钟 Bar 的起始时间（xx:00） """
        return dt.replace(minute=0, second=0, microsecond=0)

    def get_bar_start_time(self, dt: datetime) -> datetime:
        if self.timeframe == '15m':
            return self._get_bar_start_15m(dt)
        elif self.timeframe == '60m':
            return self._get_bar_start_60m(dt)
        else:
            raise ValueError(f"暂不支持的周期: {self.timeframe}")

    def update_bar(self, dt: datetime, price: float, vol_increment: float):
        """
        更新当前 Bar；若时间区间已切换，则先将旧 Bar 入库，再开启新 Bar
        :param dt: 当前行情时间
        :param price: 最新价
        :param vol_increment: 当前笔新增成交量（非累计）
        """
        bar_start = self.get_bar_start_time(dt)

        if self.current_bar_start is None:
            # 首笔数据
            self.current_bar_start = bar_start
            self.open_price = price
            self.high_price = price
            self.low_price = price
            self.close_price = price
            self.volume = vol_increment
            logging.debug(f"{self.symbol} [{self.timeframe}] 初始化 K 线: {self.current_bar_start}")
        else:
            if bar_start == self.current_bar_start:
                # 还在同一根 K 线
                if price > self.high_price:
                    self.high_price = price
                if price < self.low_price:
                    self.low_price = price
                self.close_price = price
                self.volume += vol_increment
                logging.debug(f"{self.symbol} [{self.timeframe}] 更新 K 线: {self.current_bar_start}")
            else:
                # 时间区间跳变 -> 旧 Bar 收口
                self._save_finished_bar()

                # 开新 Bar
                self.current_bar_start = bar_start
                self.open_price = price
                self.high_price = price
                self.low_price = price
                self.close_price = price
                self.volume = vol_increment
                logging.debug(f"{self.symbol} [{self.timeframe}] 开始新 K 线: {self.current_bar_start}")

    def _save_finished_bar(self):
        """
        将上一根 Bar 写入 MongoDB
        """
        if self.current_bar_start is None:
            return

        # **转换为北京时间**
        beijing_time = self.current_bar_start.astimezone(pytz.timezone("Asia/Shanghai"))

        # **转换为字符串格式：YYYY-MM-DD HH:MM:SS**
        time_str = beijing_time.strftime("%Y-%m-%d %H:%M:%S")
        
        bar_dict = {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "datetime": time_str,
            "open": self.open_price,
            "high": self.high_price,
            "low": self.low_price,
            "close": self.close_price,
            "volume": self.volume
        }
        try:
            self.db_collection.insert_one(bar_dict)
            logging.info(f"保存 K 线到 MongoDB: {bar_dict}")
        except errors.PyMongoError as e:
            logging.error(f"MongoDB 插入失败: {e}")


def is_trading_time(now: datetime) -> bool:
    """
    判断当前时间是否为交易时间（北京时间）
    """
    if now.weekday() not in TRADING_DAYS:
        return False

    time_str = now.strftime("%H:%M")
    for period in TRADING_PERIODS:
        if period["start"] <= time_str < period["end"]:
            return True
    return False


def get_realtime_quotes(symbols: list) -> pd.DataFrame:
    """
    获取指定股票的实时行情数据
    :param symbols: 股票代码列表
    :return: 包含指定股票行情的 DataFrame
    """
    try:
        df_all = ak.stock_zh_a_spot_em()
        df_selected = df_all[df_all["代码"].isin(symbols)].copy()
        return df_selected
    except Exception as e:
        logging.error(f"获取实时行情失败: {e}")
        return pd.DataFrame()  # 返回空 DataFrame 以便后续处理


def save_daily_data(db_collection_daily, symbol: str, row: pd.Series, current_date: str):
    """
    保存日线数据到 MongoDB
    :param db_collection_daily: MongoDB 中日线数据的集合
    :param symbol: 股票代码
    :param row: 行情数据行
    :param current_date: 当前日期字符串，如 '2023-10-01'
    """
    daily_record = {
        "symbol": symbol,
        "date": current_date,
        "latest_price": float(row['最新价']),
        "high": float(row['最高']),
        "low": float(row['最低']),
        "volume": float(row['成交量']) if not pd.isna(row['成交量']) else 0.0
    }
    try:
        # 使用 symbol 和 date 作为唯一键，避免重复插入
        db_collection_daily.update_one(
            {"symbol": symbol, "date": current_date},
            {"$set": daily_record},
            upsert=True
        )
        logging.info(f"保存日线数据到 MongoDB: {daily_record}")
    except errors.PyMongoError as e:
        logging.error(f"MongoDB 日线数据插入失败: {e}")


def create_indexes(collection_kline, collection_daily):
    """
    在 MongoDB 集合上创建索引
    :param collection_kline: K 线数据集合
    :param collection_daily: 日线数据集合
    """
    try:
        # 为 K 线数据创建复合索引：symbol + timeframe + datetime
        collection_kline.create_index(
            [("symbol", ASCENDING), ("timeframe", ASCENDING), ("datetime", ASCENDING)],
            unique=True,
            name="symbol_timeframe_datetime_idx"
        )
        logging.info("已在 'kline' 集合上创建索引")

        # 为日线数据创建索引：symbol + date
        collection_daily.create_index(
            [("symbol", ASCENDING), ("date", ASCENDING)],
            unique=True,
            name="symbol_date_idx"
        )
        logging.info("已在 'daily' 集合上创建索引")
    except errors.PyMongoError as e:
        logging.error(f"MongoDB 索引创建失败: {e}")


def main():
    # 连接 MongoDB 数据库
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection_kline = db[MONGO_COLLECTION_KLINE]
    collection_daily = db[MONGO_COLLECTION_DAILY]
    
    # 创建必要的索引
    create_indexes(collection_kline, collection_daily)
    
    # 创建实时数据抓取和 K 线生成器
    aggregators_15m = {symbol: TimeframeAggregator("15m", symbol, collection_kline) for symbol in SYMBOL_LIST}
    aggregators_60m = {symbol: TimeframeAggregator("60m", symbol, collection_kline) for symbol in SYMBOL_LIST}
    
    while True:
        try:
            now = datetime.now(BEIJING_TZ)
            
            if not is_trading_time(now):  # 如果当前不是交易时间，跳过
                logging.info(f"当前时间 {now.strftime('%Y-%m-%d %H:%M:%S')} 不是交易时间。")
                time.sleep(SLEEP_SEC)
                continue
            
            # 获取实时行情数据
            df_quotes = get_realtime_quotes(SYMBOL_LIST)

            if not df_quotes.empty:
                for _, row in df_quotes.iterrows():
                    symbol = row['代码']
                    last_price = row['最新价']
                    volume = row['成交量']

                    # 更新 15 分钟 K 线
                    aggregators_15m[symbol].update_bar(now, last_price, volume)
                    # 更新 60 分钟 K 线
                    aggregators_60m[symbol].update_bar(now, last_price, volume)

                # 每日结束时更新日线数据
                current_date = now.strftime("%Y-%m-%d")
                for _, row in df_quotes.iterrows():
                    symbol = row['代码']
                    save_daily_data(collection_daily, symbol, row, current_date)

            time.sleep(SLEEP_SEC)

        except KeyboardInterrupt:
            logging.info("手动中断，程序退出。")
            break
        except Exception as e:
            logging.error(f"程序运行发生异常: {e}")
            time.sleep(SLEEP_SEC)

if __name__ == "__main__":
    main()