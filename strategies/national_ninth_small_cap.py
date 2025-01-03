#国九小市值策略 测试用
from vnpy_portfoliostrategy import (StrategyTemplate,BarData,ArrayManager)
from factor_manager.manager import FactorManager
from datetime import datetime
from pymongo import MongoClient
from utils.config_loader import load_config

class ProfitFreeStrategy(StrategyTemplate):
    """
    soha策略
    """

    # 策略参数
    stock_num = 4                  # 持股数量
    stoploss_limit = 0.07          # 止损线
    highest = 60                   # 股票单价上限
    etf = "512760"            # 芯片ETF 代码
    min_mv = 3                     # 最小市值
    max_mv = 1000                  # 最大市值
    trading_signal = True          # 是否为可交易日
    run_stoploss = True            # 是否进行止损

    # 策略变量
    target_list = []               # 目标股票池
    hold_list = []                 # 当前持仓列表
    
    def __init__(self,config_path: str = 'config.yaml'):
        self.size = 20 # 存储长度
        self.data = {} # [str, ArrayManager]
        self.factor_manager = FactorManager()
        
        #baostock数据库
        self.config = load_config(config_path)
        mongo_cfg = self.config['mongodb']
        baostock_cfg = self.config['baostock']
        self.client = MongoClient(mongo_cfg['uri'])
        self.baostock_db = self.client[baostock_cfg["db"]]
        self.baostock_stock_basic_col = self.baostock_db[baostock_cfg["basic"]]
        self.baostock_daily_col = self.baostock_db[baostock_cfg["daily"]]

    def on_init(self):
        """
        Callback when strategy is initialized.
        """
        self.write_log("策略初始化")
        self.load_bars(20)

    def on_start(self):
        """
        Callback when strategy is started.
        """
        self.write_log("策略启动")

    def on_stop(self):
        """
        Callback when strategy is stopped.
        """
        self.write_log("策略停止")

    def on_bars(self, bars: BarData):
        """
        Callback of new bar data update.
        """
        stock_list = self.check_stock(bars)
        self.put_event()

    def filter_stocks(self, bars):
        """
        过滤股票池：
        
        """
        self.write_log("准备股票池")
        
        vt_symbols = set(bars.keys())
        
        stock_list = []
        
        return stock_list

    def sell_stocks(self):
        """
        止损止盈逻辑
        """
        self.write_log("止损止盈逻辑")

    def close_account(self):
        """
        清仓逻辑
        """
        self.write_log("清仓逻辑")
