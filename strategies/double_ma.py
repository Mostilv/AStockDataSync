from vnpy_ctastrategy import (
    CtaTemplate,
    BarData,
    ArrayManager
)

class DoubleMaStrategy(CtaTemplate):
    author = "YourName"

    fast_window = 10
    slow_window = 30

    parameters = ["fast_window", "slow_window"]
    variables = []

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)
        self.am = ArrayManager(max(self.fast_window, self.slow_window) + 10)

    def on_init(self):
        self.write_log("策略初始化")
        # 加载一定数量的历史K线用于初始化均线
        self.load_bar(30)

    def on_start(self):
        self.write_log("策略启动")

    def on_bar(self, bar: BarData):
        self.am.update_bar(bar)
        if not self.am.inited:
            return

        fast_ma = self.am.sma(self.fast_window, array=True)
        slow_ma = self.am.sma(self.slow_window, array=True)

        # 判断刚刚形成的K线（[-1]）和上一根K线（[-2]）的均线位置关系
        if fast_ma[-1] > slow_ma[-1] and fast_ma[-2] <= slow_ma[-2]:
            # 金叉，买入
            self.buy(bar.close_price, 1)
        elif fast_ma[-1] < slow_ma[-1] and fast_ma[-2] >= slow_ma[-2]:
            # 死叉，卖出（平仓）
            self.sell(bar.close_price, 1)

    def on_trade(self, trade):
        pass

    def on_stop_order(self, stop_order):
        pass

