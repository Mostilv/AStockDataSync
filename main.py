import os
os.environ["PYTHONIOENCODING"] = "utf-8"

from vnpy.trader.optimize import OptimizationSetting
from vnpy_ctastrategy.backtesting import BacktestingEngine
from vnpy_ctastrategy import CtaTemplate
from vnpy.trader.setting import SETTINGS

from datetime import datetime
from strategies.double_ma import DoubleMaStrategy
from data_manager.manager_tushare import TushareManager
from data_manager.manager_baostock import BaostockManager


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
    # print(1111111111111111)
    # engine.show_chart()

    # setting = OptimizationSetting()
    # setting.set_target("sharpe_ratio")
    # setting.add_parameter("atr_length", 25, 27, 1)
    # setting.add_parameter("atr_ma_length", 10, 30, 10)
    # engine.run_ga_optimization(setting)
    
    print('func main')

def manage_tushare_data():
    ts_manager = TushareManager()
    
    # ts_manager.fetch_stock_basic()
    # ts_manager.fetch_all_daily_data()
    df = ts_manager.fetch_one_day_data()
    ts_manager.save_to_mongo(df)

def manage_baostock_data():
    bs_manager = BaostockManager()
    # bs_manager.get_stock_basic_info()
    bs_manager.update_stocks_daily()
    bs_manager.close()

if __name__ == "__main__":
    manage_baostock_data()
    
    