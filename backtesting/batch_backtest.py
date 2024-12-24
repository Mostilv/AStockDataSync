# batch_backtest.py

import pandas as pd
import concurrent.futures
from tqdm import tqdm
from typing import List, Dict
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

from backtesting.backtest_single import run_single_backtest

# 1) 把原本在 run_batch_backtest 内的 worker 函数，提到全局层面
def worker_task(symbol: str, strategy_module: str, strategy_class_name: str, 
                start_date: str, end_date: str, capital: float,
                rate: float, slippage: float, size: int, pricetick: float, 
                strategy_params: Dict) -> pd.DataFrame:
    """
    供 ProcessPoolExecutor 调用的全局函数。
    """
    df_res = run_single_backtest(
        vt_symbol=symbol,
        strategy_module=strategy_module,
        strategy_class_name=strategy_class_name,
        start_date=start_date,
        end_date=end_date,
        capital=capital,
        rate=rate,
        slippage=slippage,
        size=size,
        pricetick=pricetick,
        strategy_params=strategy_params
    )
    return df_res


def run_batch_backtest(
    symbol_list: List[str],
    strategy_module: str,
    strategy_class_name: str,
    start_date: str,
    end_date: str,
    capital: float = 1_000_000,
    rate: float = 0.0002,
    slippage: float = 0.01,
    size: int = 1,
    pricetick: float = 0.01,
    strategy_params: Dict = None,
    max_workers: int = 4
) -> pd.DataFrame:
    """
    批量回测，并使用 tqdm 展示进度。
    返回包含 "average_capital" 列的 DataFrame，为每日平均资金曲线。
    """
    symbol_capital_map = {}

    # 2) 构造任务列表
    tasks = []
    for sym in symbol_list:
        tasks.append((sym, strategy_module, strategy_class_name, start_date, end_date,
                      capital, rate, slippage, size, pricetick, strategy_params))

    # 3) 并行执行
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        # 提交任务
        future_to_symbol = {
            executor.submit(worker_task, *t): t[0] for t in tasks
        }
        with tqdm(total=len(tasks), desc="Batch Backtest", unit="stock") as pbar:
            for future in concurrent.futures.as_completed(future_to_symbol):
                sym = future_to_symbol[future]
                try:
                    df_res = future.result()
                    symbol_capital_map[sym] = df_res
                except Exception as e:
                    print(f"[Error] 回测失败: {sym}, error={e}")
                finally:
                    pbar.update(1)

    # 4) 合并计算平均资金
    if not symbol_capital_map:
        print("No results. Check data or strategy.")
        return pd.DataFrame()

    # 过滤掉回测失败的标的
    valid_results = {sym: df_capital for sym, df_capital in symbol_capital_map.items() if not df_capital.empty}

    if not valid_results:
        print("No valid results. All backtests failed.")
        return pd.DataFrame()

    # 合并有效的资金曲线
    df_list = []
    for sym, df_capital in valid_results.items():
        # 将资金列重命名为对应标的名称
        df_tmp = df_capital.rename(columns={"capital": sym})
        df_list.append(df_tmp)

    # 按日期对齐，合并为一个 DataFrame
    merged_df = pd.concat(df_list, axis=1, join="outer")

    # 计算每日的平均资金曲线
    merged_df["average_capital"] = merged_df.mean(axis=1)

    return merged_df[["average_capital"]]


def plot_average_capital_curve(df: pd.DataFrame, title: str = "Average Capital Curve"):
    """
    绘制批量回测后的平均资金曲线
    """
    if "average_capital" not in df.columns:
        print("[Error] df缺少 'average_capital' 列")
        return

    df.sort_index(inplace=True)
    plt.figure(figsize=(10, 6))
    plt.plot(df.index, df["average_capital"], label="Avg Capital")
    plt.title(title)
    plt.xlabel("Date")
    plt.ylabel("Capital")
    plt.legend()
    plt.grid(True)
    
    # 设置纵坐标完整显示（无科学计数法）
    ax = plt.gca()
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{int(x):,}"))  # 格式化为带逗号的完整数字
    
    plt.show()
