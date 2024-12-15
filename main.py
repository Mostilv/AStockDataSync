from vnpy_ctastrategy.backtesting import BacktestingEngine, OptimizationSetting
from vnpy_ctastrategy import CtaTemplate
from vnpy.trader.setting import SETTINGS

from datetime import datetime

from data_manager.run import run_pipeline
from strategies.double_ma import DoubleMaStrategy



import os
os.environ["PYTHONIOENCODING"] = "utf-8"

SETTINGS["database.name"] = "mongodb"
SETTINGS["database.database"] = "vnpy"
SETTINGS["database.host"] = "127.0.0.1"
SETTINGS["database.port"] = 27017

def main():
    # engine = BacktestingEngine()
    # engine.set_parameters(
    #     vt_symbol="600435.SSE",
    #     interval="d",
    #     start=datetime(2020, 1, 1),
    #     end=datetime(2020, 12, 31),
    #     rate=0.0005,
    #     slippage=0.2,
    #     size=100,
    #     pricetick=0.01,
    #     capital=1_000_000,
    # )
    # engine.add_strategy(DoubleMaStrategy, {"fast_window": 10, "slow_window": 20})
    # engine.load_data()
    # engine.run_backtesting()
    # df = engine.calculate_result()
    # statistics = engine.calculate_statistics(output=True)
    # engine.show_chart()
    print('func main')

if __name__ == "__main__":
    main()
    