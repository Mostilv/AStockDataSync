# backtest_factor_strategy.py

from datetime import datetime

# vn.py 相关
from vnpy_ctastrategy.backtesting import BacktestingEngine
from vnpy.trader.setting import SETTINGS

# 策略示例
from strategies.test_factor_strategy import TestFactorStrategy

# 第三方可视化库
import matplotlib.pyplot as plt

# 1) 配置数据库连接（MongoDB）
SETTINGS["database.name"] = "mongodb"
SETTINGS["database.database"] = "vnpy"
SETTINGS["database.host"] = "127.0.0.1"
SETTINGS["database.port"] = 27017

def plot_backtest_result(df):
    """
    使用 Matplotlib 绘制回测的资金曲线等指标。
    :param df: engine.calculate_result() 返回的 DataFrame，
    一般包含 columns: ["date", "balance", "net_pnl", "drawdown", ...]
    """
    # 设置绘图风格
    print(plt.style.available)  # 查看当前可用的样式列表
    plt.style.use("ggplot")     # 例如 "ggplot" 是内置可用的样式
    fig, ax = plt.subplots(figsize=(10, 6))

    # 如果 df 中有 date 列，就以 date 作为 x 轴；否则用索引
    if "date" in df.columns:
        x_data = df["date"]
    else:
        x_data = df.index  # 回退到默认索引

    # 绘制资金曲线
    ax.plot(x_data, df["balance"], label="Balance Curve", color="blue", linewidth=1.0)

    # （可选）再绘制回撤
    if "drawdown" in df.columns:
        ax.plot(x_data, df["drawdown"], label="Drawdown", color="red", linewidth=1.0)
    
    ax.set_title("Backtest Result")
    ax.set_xlabel("Date")
    ax.set_ylabel("Balance")
    ax.grid(True)
    ax.legend()

    plt.show()


def run_backtest():
    engine = BacktestingEngine()
    engine.set_parameters(
        vt_symbol="600000.SSE",    # 模拟一个股票代码
        interval="d",              # 使用日线
        start=datetime(2014, 1, 1),
        end=datetime(2024, 12, 1),
        rate=0.0003,               # 手续费
        slippage=0.01,             # 滑点
        size=100,                  # 一手=100股
        pricetick=0.01,
        capital=100000
    )

    # 如果你有CSV可以直接加载，也可以注释掉
    # engine.add_data(TestFactorStrategy, "your_data.csv")

    # 添加策略
    engine.add_strategy(
        TestFactorStrategy,
        {
            "rsi_threshold_buy": 30,
            "rsi_threshold_sell": 70,
            "fixed_size": 1
        }
    )

    # 加载数据 & 运行回测
    engine.load_data()
    engine.run_backtesting()

    # 2) 获取回测结果
    df = engine.calculate_result()

    # 3) 打印统计结果(收益、回撤等)，并绘图
    engine.calculate_statistics(output=True)
    plot_backtest_result(df)


if __name__ == "__main__":
    run_backtest()

