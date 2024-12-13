from data_manager.tushare import TushareManager

# 获取今天的日期并格式化为 yyyymmdd
manager = TushareManager()

manager.fetch_stock_basic()