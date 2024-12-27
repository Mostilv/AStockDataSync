"""
main.py

1) 配置 vnpy 的 MongoDB 数据库
2) 从 vnpy 库中的 BarOverview 表读取所有股票代码
3) 调用批量回测函数
4) 绘制平均资金曲线
"""
from vnpy_mongodb import Database
from vnpy.trader.setting import SETTINGS
from vnpy.trader.constant import Exchange, Interval

# 1) 配置 MongoDB
SETTINGS["database.name"] = "mongodb"
SETTINGS["database.database"] = "vnpy"
SETTINGS["database.host"] = "127.0.0.1"
SETTINGS["database.port"] = 27017
# 如果有用户名/密码，请补充：
# SETTINGS["database.user"] = "..."
# SETTINGS["database.password"] = "..."

# 2) 导入批量回测函数
from backtesting.batch_backtest import run_batch_backtest, plot_average_capital_curve, process_statistics


def read_all_stock_symbols() -> list:
    """
    从 vnpy 数据库的 BarOverview 集合获取所有股票代码列表（vt_symbol）。
    只保留日线数据，且交易所是 SSE/SZSE（上证/深证）。
    返回格式形如 ["600435.SSE", "301603.SZSE", ...].
    """
    db = Database()
    overviews = db.get_bar_overview()  # 返回 List[BarOverview]

    symbol_list = []
    for ov in overviews:
        # 1) 只保留 SSE(上证) / SZSE(深证)
        # 2) 只保留日线 Interval.DAILY
        if ov.exchange in [Exchange.SSE, Exchange.SZSE] and ov.interval == Interval.DAILY:
            # 拼成 "symbol.exchange" 形式
            vt_symbol = f"{ov.symbol}.{ov.exchange.value}"
            symbol_list.append(vt_symbol)

    return symbol_list


def main():
    """
    项目主入口
    1) 从数据库读取所有股票代码
    2) 调用批量回测函数
    3) 绘制平均资金曲线
    """
    print("=== 读取股票代码 ===")
    symbol_list = read_all_stock_symbols()
    print(f"共获取到 {len(symbol_list)} 条股票代码。")

    # 这里示例只回测前 10 个股票，避免机器压力过大
    # 若要全股票回测，请去掉下面这行
    symbol_list = symbol_list[:10]

    print("=== 开始批量回测 ===")
    df_capital, df_stats = run_batch_backtest(
        symbol_list=symbol_list,
        strategy_module="strategies.test_factor_strategy",   # 需要你自己在 strategies 下写好 demo_strategy.py
        strategy_class_name="TestFactorStrategy",           # demo_strategy.py 中的策略类名
        start_date="2014-01-01",
        end_date="2024-01-01",
        capital=1_000_000,
        max_workers=4
    )

    # 打印统计指标
    process_statistics(df_stats)
    # 绘制资金曲线
    plot_average_capital_curve(df_capital)

    print("=== 结束 ===")


if __name__ == "__main__":
    main()

