# from data_manager.tushare import TushareManager

# # 获取今天的日期并格式化为 yyyymmdd
# manager = TushareManager()

# # manager.fetch_stock_basic()
# manager.fetch_all_daily_data()
import akshare as ak

# 获取上证指数（代码：sh000001）60 分钟级别的历史数据
stock_data = ak.stock_zh_a_minute(symbol='sh000001', period='15')
print(stock_data)