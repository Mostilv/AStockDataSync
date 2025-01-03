from datetime import datetime
from importlib import reload

import vnpy_portfoliostrategy
reload(vnpy_portfoliostrategy)

from vnpy_portfoliostrategy import BacktestingEngine
from vnpy.trader.constant import Interval


from vnpy_mongodb import Database
from vnpy.trader.setting import SETTINGS
from vnpy.trader.constant import Exchange, Interval

from strategies.portfolio_boll_channel_strategy import PortfolioBollChannelStrategy
#----------------------------------------------------------------------------------
SETTINGS["database.name"] = "mongodb"
SETTINGS["database.database"] = "vnpy"
SETTINGS["database.host"] = "127.0.0.1"
SETTINGS["database.port"] = 27017

def get_all_stock_symbols() -> list:
    """
    从 vnpy 数据库的 BarOverview 集合获取所有股票代码列表（vt_symbol）。
    只保留日线数据，且交易所是 SSE/SZSE（上证/深证）。
    返回格式形如 ["600435.SSE", "301603.SZSE", ...].
    """
    db = Database()
    overviews = db.get_bar_overview()  # 返回 List[BarOverview]

    vt_symbols = []
    for ov in overviews:
        # 1) 只保留 SSE(上证) / SZSE(深证)
        # 2) 只保留日线 Interval.DAILY
        if ov.exchange in [Exchange.SSE, Exchange.SZSE] and ov.interval == Interval.DAILY:
            # 拼成 "symbol.exchange" 形式
            vt_symbol = f"{ov.symbol}.{ov.exchange.value}"
            vt_symbols.append(vt_symbol)

    return vt_symbols[:100]

vt_symbols = get_all_stock_symbols()
# 为每个股票生成默认参数，以下是生成示例，你可以根据实际情况修改每个股票的参数
rates = {symbol: 0/10000 for symbol in vt_symbols}  # 假设所有股票的手续费都一样
slippages = {symbol: 0 for symbol in vt_symbols}  # 假设所有股票的滑点都一样
sizes = {symbol: 10 for symbol in vt_symbols}  # 假设每个股票的交易单位都一样
priceticks = {symbol: 1 for symbol in vt_symbols}  # 假设每个股票的最小价格变动都一样

engine = BacktestingEngine()
engine.set_parameters(
    vt_symbols=vt_symbols,
    interval=Interval.DAILY,
    start=datetime(2022, 1, 1),
    end=datetime(2024, 12, 30),
    rates=rates,  # 所有股票的手续费
    slippages=slippages,  # 所有股票的滑点
    sizes=sizes,  # 所有股票的交易单位
    priceticks=priceticks,  # 所有股票的最小价格变动
    capital=1_000_000,  # 初始资金
)

setting = {
}
engine.add_strategy(PortfolioBollChannelStrategy, setting)

engine.load_data()
engine.run_backtesting()
df = engine.calculate_result()
engine.calculate_statistics()
engine.show_chart()