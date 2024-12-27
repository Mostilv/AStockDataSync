# backtest_single.py

import importlib
from datetime import datetime
import pandas as pd

from vnpy_ctastrategy.backtesting import BacktestingEngine
from vnpy_ctastrategy.base import BacktestingMode


def run_single_backtest(
    vt_symbol: str,
    strategy_module: str,
    strategy_class_name: str,
    start_date: str,
    end_date: str,
    capital: float = 1_000_000,
    rate: float = 0.0002,
    slippage: float = 0.01,
    size: int = 1,
    pricetick: float = 0.01,
    strategy_params: dict = None
) -> (pd.DataFrame, dict):
    """
    使用 vnpy BacktestingEngine 对单支股票进行回测，返回每日资金曲线 + 统计指标。
    :return: (df_result, stats_dict)
        df_result: pd.DataFrame(["capital"]), index=datetime
        stats_dict: 统计结果的 dict
    """

    # 1) 动态导入策略类
    module = importlib.import_module(strategy_module)
    strategy_class = getattr(module, strategy_class_name)

    # 2) 创建回测引擎
    engine = BacktestingEngine()
    engine.set_parameters(
        vt_symbol=vt_symbol,
        interval="d",  # 日线
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
    engine.load_data()        # 从 vnpy DB 读取数据
    engine.run_backtesting()  # 执行回测

    # 3) 计算每日结果、每日资金
    df_daily = engine.calculate_result()
    df_daily["cum_net_pnl"] = df_daily["net_pnl"].cumsum()  # 累计净盈亏
    df_daily["capital"] = capital + df_daily["cum_net_pnl"]  # 账户资金
    df_daily.index.name = "datetime"
    df_result = df_daily[["capital"]].copy()

    # 4) 计算统计指标
    stats_dict = engine.calculate_statistics(output=False)  # 不打印输出

    return df_result, stats_dict
