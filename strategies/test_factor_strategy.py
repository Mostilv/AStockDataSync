# strategies/test_factor_strategy.py

from vnpy_ctastrategy import (
    CtaTemplate,
    BarData
)
from vnpy.trader.object import TickData
from vnpy.trader.utility import ArrayManager
from factor_manager.manager import FactorManager

class TestFactorStrategy(CtaTemplate):
    """
    一个示例策略：使用ArrayManager的close_array来计算RSI,MACD等因子值，
    并根据简单阈值进行买卖。
    """
    author = "MyQuant"

    # 策略参数(可视化时可调)
    rsi_threshold_buy = 30
    rsi_threshold_sell = 70
    fixed_size = 1

    # 策略变量
    rsi_value = 0.0
    macd_value = 0.0

    parameters = ["rsi_threshold_buy", "rsi_threshold_sell", "fixed_size"]
    variables = ["rsi_value", "macd_value"]

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)

        self.am = ArrayManager(size=100)
        self.factor_manager = FactorManager()

    def on_init(self):
        """
        Callback when strategy is initialized.
        """
        self.write_log("策略初始化")
        self.load_bar(10)  # 加载10根历史K线，用于初始化

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

    def on_tick(self, tick: TickData):
        """
        Callback of new tick data update.
        """
        pass

    def on_bar(self, bar: BarData):
        """
        Callback of new bar data update.
        """
        # 先撤销所有委托
        self.cancel_all()

        # 更新ArrayManager
        self.am.update_bar(bar)
        if not self.am.inited:
            return  # 数据尚不足以计算

        # 计算因子
        close_array = self.am.close  # numpy array
        factor_result = self.factor_manager.calculate_factors(
            close_array, 
            factor_names=["RSI", "MACD"]
        )
        self.rsi_value = factor_result.get("RSI", None)
        self.macd_value = factor_result.get("MACD", None)

        # 简单的买卖逻辑
        if self.rsi_value is not None and self.rsi_value < self.rsi_threshold_buy:
            self.buy(price=bar.close_price, volume=self.fixed_size)
        elif self.rsi_value is not None and self.rsi_value > self.rsi_threshold_sell:
            self.sell(price=bar.close_price, volume=self.fixed_size)

        # 推送策略状态
        self.put_event()
