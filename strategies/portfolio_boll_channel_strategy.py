from datetime import datetime

from vnpy.trader.utility import ArrayManager, Interval
from vnpy.trader.object import TickData, BarData

from vnpy_portfoliostrategy import StrategyTemplate, StrategyEngine
from vnpy_portfoliostrategy.utility import PortfolioBarGenerator


class PortfolioBollChannelStrategy(StrategyTemplate):
    """组合布林带通道策略"""

    author = "用Python的交易员"

    boll_window = 18
    boll_dev = 3.4
    cci_window = 10
    atr_window = 30
    sl_multiplier = 5.2
    fixed_size = 1
    price_add = 5

    parameters = [
        "boll_window",
        "boll_dev",
        "cci_window",
        "atr_window",
        "sl_multiplier",
        "fixed_size",
        "price_add"
    ]
    variables = []

    def __init__(
        self,
        strategy_engine: StrategyEngine,
        strategy_name: str,
        vt_symbols: list[str],
        setting: dict
    ) -> None:
        """构造函数"""
        super().__init__(strategy_engine, strategy_name, vt_symbols, setting)

        self.boll_up: dict[str, float] = {}
        self.boll_down: dict[str, float] = {}
        self.cci_value: dict[str, float] = {}
        self.atr_value: dict[str, float] = {}
        self.intra_trade_high: dict[str, float] = {}
        self.intra_trade_low: dict[str, float] = {}

        self.targets: dict[str, int] = {}
        self.last_tick_time: datetime = None

        # 获取合约信息
        self.ams: dict[str, ArrayManager] = {}
        for vt_symbol in self.vt_symbols:
            self.ams[vt_symbol] = ArrayManager()
            self.targets[vt_symbol] = 0

    def on_init(self) -> None:
        """策略初始化回调"""
        self.write_log("策略初始化")

        self.load_bars(10)

    def on_start(self) -> None:
        """策略启动回调"""
        self.write_log("策略启动")

    def on_stop(self) -> None:
        """策略停止回调"""
        self.write_log("策略停止")

    def on_tick(self, tick: TickData) -> None:
        """行情推送回调"""
        self.pbg.update_tick(tick)

    def on_bars(self, bars: dict[str, BarData]) -> None:
        self.cancel_all()

        # 确保每个符号的数据存在
        for vt_symbol in self.vt_symbols:
            if vt_symbol not in bars:
                self.write_log(f"Symbol {vt_symbol} missing in bars data, skipping.")
                continue  # 如果数据没有传入，跳过这个符号

            # 更新到缓存序列
            bar = bars[vt_symbol]
            am: ArrayManager = self.ams[vt_symbol]
            am.update_bar(bar)

        # 遍历所有符号并计算相关指标
        for vt_symbol in bars.keys():
            # 确保符号数据已经初始化
            if vt_symbol not in self.boll_up:
                self.boll_up[vt_symbol] = None
            if vt_symbol not in self.boll_down:
                self.boll_down[vt_symbol] = None
            if vt_symbol not in self.cci_value:
                self.cci_value[vt_symbol] = None
            if vt_symbol not in self.atr_value:
                self.atr_value[vt_symbol] = None

            # 确保技术指标已计算
            am: ArrayManager = self.ams[vt_symbol]
            if not am.inited:
                self.write_log(f"Symbol {vt_symbol} ArrayManager not initialized, skipping.")
                continue  # 如果数据没有初始化，则跳过当前符号

            # 计算布林带、CCI、ATR指标
            self.boll_up[vt_symbol], self.boll_down[vt_symbol] = am.boll(self.boll_window, self.boll_dev)
            self.cci_value[vt_symbol] = am.cci(self.cci_window)
            self.atr_value[vt_symbol] = am.atr(self.atr_window)

            # 检查布林带计算是否失败
            if self.boll_up[vt_symbol] is None or self.boll_down[vt_symbol] is None:
                self.write_log(f"Bollinger Bands calculation failed for {vt_symbol}, skipping.")
                continue

            # 计算目标仓位
            current_pos = self.get_pos(vt_symbol)
            if current_pos == 0:
                bar = bars[vt_symbol]
                self.intra_trade_high[vt_symbol] = bar.high_price
                self.intra_trade_low[vt_symbol] = bar.low_price

                if self.cci_value[vt_symbol] > 0:
                    self.targets[vt_symbol] = self.fixed_size
                elif self.cci_value[vt_symbol] < 0:
                    self.targets[vt_symbol] = -self.fixed_size

            elif current_pos > 0:
                bar = bars[vt_symbol]
                self.intra_trade_high[vt_symbol] = max(self.intra_trade_high[vt_symbol], bar.high_price)
                self.intra_trade_low[vt_symbol] = bar.low_price

                long_stop = self.intra_trade_high[vt_symbol] - self.atr_value[vt_symbol] * self.sl_multiplier

                if bar.close_price <= long_stop:
                    self.targets[vt_symbol] = 0

            elif current_pos < 0:
                bar = bars[vt_symbol]
                self.intra_trade_low[vt_symbol] = min(self.intra_trade_low[vt_symbol], bar.low_price)
                self.intra_trade_high[vt_symbol] = bar.high_price

                short_stop = self.intra_trade_low[vt_symbol] + self.atr_value[vt_symbol] * self.sl_multiplier

                if bar.close_price >= short_stop:
                    self.targets[vt_symbol] = 0

        # 基于目标仓位进行委托
        for vt_symbol in self.vt_symbols:
            if vt_symbol not in self.targets:
                self.write_log(f"Missing target position for {vt_symbol}, skipping.")
                continue  # 如果没有目标仓位，跳过这个符号

            target_pos = self.targets[vt_symbol]
            current_pos = self.get_pos(vt_symbol)

            pos_diff = target_pos - current_pos
            volume = abs(pos_diff)
            bar = bars.get(vt_symbol)

            if bar is None:
                self.write_log(f"Missing bar data for {vt_symbol}, skipping.")
                continue  # 如果没有对应的bar数据，跳过此符号

            boll_up = self.boll_up.get(vt_symbol)
            boll_down = self.boll_down.get(vt_symbol)

            if pos_diff > 0:
                price = bar.close_price + self.price_add

                if current_pos < 0:
                    print('cover')
                    self.cover(vt_symbol, price, volume)
                else:
                    print('buy')
                    self.buy(vt_symbol, boll_up, volume)

            elif pos_diff < 0:
                price = bar.close_price - self.price_add

                if current_pos > 0:
                    print('sell')
                    self.sell(vt_symbol, price, volume)
                else:
                    print('short')
                    self.short(vt_symbol, boll_down, volume)

        # 推送界面更新
        self.put_event()
