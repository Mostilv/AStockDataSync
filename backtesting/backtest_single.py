# backtest_single.py

import importlib
from datetime import datetime
import pandas as pd

from vnpy_ctastrategy.backtesting import BacktestingEngine
from vnpy_ctastrategy.base import BacktestingMode


def run_single_backtest(
    vt_symbol: str,
    strategy_module: str,       # 如 "strategies.demo_strategy"
    strategy_class_name: str,   # 如 "DemoStrategy"
    start_date: str,
    end_date: str,
    capital: float = 1_000_000,
    rate: float = 0.0002,
    slippage: float = 0.01,
    size: int = 1,
    pricetick: float = 0.01,
    strategy_params: dict = None
) -> pd.DataFrame:
    """
    使用 vnpy BacktestingEngine 对单支股票进行回测，返回每日资金曲线。

    :param vt_symbol: vnpy 格式代码，如 "000001.SZSE"
    :param strategy_module: 策略所在的python模块字符串
    :param strategy_class_name: 策略类名字符串
    :param start_date: "YYYY-MM-DD"
    :param end_date: "YYYY-MM-DD"
    :param capital: 初始资金
    :param rate: 手续费率
    :param slippage: 滑点
    :param size: 合约乘数（股票通常为1）
    :param pricetick: 最小变动价位
    :param strategy_params: 策略参数字典
    :return: pd.DataFrame，列为 ["datetime", "capital"]，index为 datetime
    """

    # 1) 动态导入策略类
    module = importlib.import_module(strategy_module)
    strategy_class = getattr(module, strategy_class_name)

    engine = BacktestingEngine()
    engine.set_parameters(
        vt_symbol=vt_symbol,
        interval="d",  # 股票日线回测
        start=datetime.strptime(start_date, "%Y-%m-%d"),
        end=datetime.strptime(end_date, "%Y-%m-%d"),
        rate=rate,
        slippage=slippage,
        size=size,
        pricetick=pricetick,
        capital=capital,
        mode=BacktestingMode.BAR
    )

    engine.add_strategy(strategy_class, strategy_params or {})
    engine.load_data()        # 从已配置好的MongoDB读取数据
    engine.run_backtesting()  # 执行回测

    # 计算每日盈亏
    df_daily = engine.calculate_result()
    df_daily["cum_net_pnl"] = df_daily["net_pnl"].cumsum()  # 累计净盈亏
    df_daily["capital"] = capital + df_daily["cum_net_pnl"]  # 账户资金

    # 设置时间索引
    df_daily.index.name = "datetime"
    df_result = df_daily[["capital"]].copy()

    return df_result
