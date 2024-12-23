# backtest_factor_strategy.py

from vnpy_ctastrategy.backtesting import BacktestingEngine
from vnpy.trader.setting import SETTINGS
from datetime import datetime

from strategies.test_factor_strategy import TestFactorStrategy

SETTINGS["database.name"] = "mongodb"
SETTINGS["database.database"] = "vnpy"
SETTINGS["database.host"] = "127.0.0.1"
SETTINGS["database.port"] = 27017

def run_backtest():
    engine = BacktestingEngine()
    engine.set_parameters(
        vt_symbol="600000.SSE",    # 模拟一个股票代码
        interval="d",             # 使用日线
        start=datetime(2014, 1, 1),
        end=datetime(2024, 12, 1),
        rate=0.0003,               # 手续费
        slippage=0.01,             # 滑点
        size=100,                  # 一手100股
        pricetick=0.01,
        capital=100000
    )

    # 这里演示从CSV加载bar数据，如果没有CSV可注释掉自行提供数据
    # engine.add_data(TestFactorStrategy, "your_data.csv")

    # 添加策略
    engine.add_strategy(TestFactorStrategy, {
        "rsi_threshold_buy": 30,
        "rsi_threshold_sell": 70,
        "fixed_size": 1
    })

    engine.load_data()
    engine.run_backtesting()
    df = engine.calculate_result()
    engine.calculate_statistics(output=True)
    engine.show_chart(df)

if __name__ == "__main__":
    run_backtest()
