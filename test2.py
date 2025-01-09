import akshare as ak
from pymongo import MongoClient, errors
import pytz

# MongoDB 连接配置
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "akshare_realtime"
MONGO_COLLECTION_KLINE = "kline"

# 定义时区
UTC_TZ = pytz.utc
BEIJING_TZ = pytz.timezone("Asia/Shanghai")  # 北京时间（CST, UTC+8）

def convert_to_beijing_time(utc_dt):
    """
    将 UTC 时间转换为北京时间
    """
    if utc_dt.tzinfo is None:
        utc_dt = utc_dt.replace(tzinfo=UTC_TZ)  # 假设存储时区缺失，默认是 UTC
    return utc_dt.astimezone(BEIJING_TZ)  # 转换为北京时间

def main():
    try:
        # 1. 连接 MongoDB
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        db = client[MONGO_DB]
        collection_kline = db[MONGO_COLLECTION_KLINE]
        print("成功连接到 MongoDB.")
    except errors.ConnectionFailure as e:
        print(f"无法连接到 MongoDB: {e}")
        return
    
    # 2. 查询 kline 集合中的第一条数据
    doc = collection_kline.find_one()
    
    if doc:
        print("原始数据:", doc)

        # 3. 提取并转换 datetime
        if "datetime" in doc:
            utc_time = doc["datetime"]  # MongoDB 存储的 datetime
            beijing_time = convert_to_beijing_time(utc_time)
            print("北京时间:", beijing_time)  # 输出转换后的北京时间
        else:
            print("文档中没有 datetime 字段。")
    else:
        print("kline 集合中没有数据。")

if __name__ == "__main__":
    main()

