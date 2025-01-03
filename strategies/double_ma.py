from datetime import datetime
import numpy as np
from vnpy.trader.utility import BarGenerator
from vnpy.trader.object import BarData
from vnpy_portfoliostrategy import StrategyTemplate, StrategyEngine

class SimpleMovingAverageStrategy(StrategyTemplate):
    """简单双均线策略，应用于全市场股票"""

    author = "用Python的交易员"
    
    short_window = 20     # 短期均线
    long_window = 60      # 长期均线
    fixed_size = 1        # 每次买入/卖出的数量

    parameters = [
        "short_window",
        "long_window",
        "fixed_size",
    ]
    
    variables = [
        "short_window",
        "long_window",
    ]

    def __init__(
        self,
        strategy_engine: StrategyEngine,
        strategy_name: str,
        vt_symbols: list[str],
        setting: dict
    ) -> None:
        """构造函数"""
        super().__init__(strategy_engine, strategy_name, vt_symbols, setting)

        self.bgs: dict[str, BarGenerator] = {}
        self.last_tick_time: datetime = None

        # 每个股票的短期和长期均线
        self.short_moving_avg = {}
        self.long_moving_avg = {}

        def on_bar(bar: BarData):
            """""" 
            pass

        # 初始化BarGenerator
        for vt_symbol in self.vt_symbols:
            self.bgs[vt_symbol] = BarGenerator(on_bar)

    def on_init(self) -> None:
        """策略初始化回调"""
        self.write_log("策略初始化")
        self.load_bars(1)

    def on_start(self) -> None:
        """策略启动回调"""
        self.write_log("策略启动")

    def on_stop(self) -> None:
        """策略停止回调"""
        self.write_log("策略停止")

    def on_bars(self, bars: dict[str, BarData]) -> None:
        """K线切片回调"""
        for vt_symbol, bar in bars.items():
            # 更新均线数据
            if vt_symbol not in self.short_moving_avg:
                self.short_moving_avg[vt_symbol] = [bar.close_price]
            else:
                self.short_moving_avg[vt_symbol].append(bar.close_price)

            if vt_symbol not in self.long_moving_avg:
                self.long_moving_avg[vt_symbol] = [bar.close_price]
            else:
                self.long_moving_avg[vt_symbol].append(bar.close_price)

            # 保持均线序列的长度
            if len(self.short_moving_avg[vt_symbol]) > self.short_window:
                self.short_moving_avg[vt_symbol].pop(0)
            if len(self.long_moving_avg[vt_symbol]) > self.long_window:
                self.long_moving_avg[vt_symbol].pop(0)

            # 计算当前的短期和长期均线
            short_avg = np.mean(self.short_moving_avg[vt_symbol])
            long_avg = np.mean(self.long_moving_avg[vt_symbol])

            # 策略逻辑：双均线策略，短期均线上穿长期均线时买入，短期均线下穿长期均线时卖出
            if short_avg > long_avg and self.get_pos(vt_symbol) == 0:
                # 买入信号
                self.set_target(vt_symbol, self.fixed_size)
            elif short_avg < long_avg and self.get_pos(vt_symbol) > 0:
                # 卖出信号
                self.set_target(vt_symbol, 0)
        # 推送更新事件
        self.put_event()
