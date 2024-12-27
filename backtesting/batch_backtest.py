# batch_backtest.py

import pandas as pd
import concurrent.futures
import os
from tqdm import tqdm
from typing import List, Dict, Tuple
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

from backtesting.backtest_single import run_single_backtest


# 1) 回测任务函数
def worker_task(
    symbol: str,
    strategy_module: str,
    strategy_class_name: str,
    start_date: str,
    end_date: str,
    capital: float,
    rate: float,
    slippage: float,
    size: int,
    pricetick: float,
    strategy_params: Dict
) -> Tuple[pd.DataFrame, dict]:
    """
    供 ProcessPoolExecutor 调用的全局函数，单支股票的回测任务。
    :return: (df_result, stats_dict)
    """
    df_res, stats = run_single_backtest(
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
    return df_res, stats


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
) -> (pd.DataFrame, pd.DataFrame):
    """
    批量回测，并使用 tqdm 展示进度。
    :return: (df_average_capital, df_stats)
        df_average_capital: 包含 'average_capital' 列的每日平均资金曲线
        df_stats: 各标的及平均的统计指标表
    """
    symbol_capital_map = {}
    symbol_stat_map = {}

    # 1) 构造任务列表
    tasks = []
    for sym in symbol_list:
        tasks.append((
            sym,
            strategy_module,
            strategy_class_name,
            start_date,
            end_date,
            capital,
            rate,
            slippage,
            size,
            pricetick,
            strategy_params
        ))

    # 2) 并行执行回测
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_symbol = {
            executor.submit(worker_task, *t): t[0] for t in tasks
        }

        with tqdm(total=len(tasks), desc="Batch Backtest", unit="stock") as pbar:
            for future in concurrent.futures.as_completed(future_to_symbol):
                sym = future_to_symbol[future]
                try:
                    df_res, stats = future.result()
                    symbol_capital_map[sym] = df_res
                    symbol_stat_map[sym] = stats
                except Exception as e:
                    print(f"[Error] 回测失败: {sym}, error={e}")
                finally:
                    pbar.update(1)

    # 若无有效结果
    if not symbol_capital_map:
        print("[Error] 无资金曲线结果")
        return pd.DataFrame(), pd.DataFrame()

    # 3) 合并计算平均资金曲线
    valid_results = {
        sym: df_capital
        for sym, df_capital in symbol_capital_map.items()
        if not df_capital.empty
    }

    if not valid_results:
        print("[Error] 全部回测失败，无法计算平均资金")
        return pd.DataFrame(), pd.DataFrame()

    df_list = []
    for sym, df_capital in valid_results.items():
        # 将资金列重命名为对应标的名称
        df_tmp = df_capital.rename(columns={"capital": sym})
        df_list.append(df_tmp)

    merged_df = pd.concat(df_list, axis=1, join="outer")
    merged_df["average_capital"] = merged_df.mean(axis=1)
    df_average_capital = merged_df[["average_capital"]].copy()

    # 4) 汇总统计指标，并计算“平均统计”
    df_stats = pd.DataFrame(symbol_stat_map).T  # 转为 DataFrame，index=股票代码, columns=统计字段
    df_stats = df_stats.sort_index()  # 排个序更美观

    # 这里示例：对所有可数值字段做“平均”处理，生成名为 "Average" 的行
    numeric_cols = df_stats.select_dtypes(include=["number"]).columns
    # 仅对数值列做平均
    avg_row = df_stats[numeric_cols].mean(axis=0, numeric_only=True)

    # 也可以根据需要，对部分字段执行加总，而非平均
    # 例如 total_net_pnl 总盈亏，可以做合计: sum_net_pnl = df_stats["total_net_pnl"].sum()
    # 下面演示就仅做平均:
    
    # 把这行命名为 "Average" 并加入 df_stats
    avg_row.name = "Average"
    df_stats = pd.concat([df_stats, avg_row.to_frame().T], axis=0)

    return df_average_capital, df_stats


def plot_average_capital_curve(df: pd.DataFrame, title: str = "Average Capital Curve"):
    """
    绘制批量回测后的平均资金曲线
    """
    if "average_capital" not in df.columns:
        print("[Error] df缺少 'average_capital' 列")
        return

    df = df.sort_index().copy()
    plt.figure(figsize=(10, 6))
    plt.plot(df.index, df["average_capital"], label="Avg Capital")
    plt.title(title)
    plt.xlabel("Date")
    plt.ylabel("Capital")
    plt.legend()
    plt.grid(True)

    # 设置纵坐标完整显示（无科学计数法）
    ax = plt.gca()
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{int(x):,}"))
    plt.show()

def process_statistics(df_stats):
    """
    对回测统计指标表进行数据处理和计算平均值。

    :param df_stats: pd.DataFrame, 包含回测统计指标的 DataFrame。
    :return: 处理后的统计指标表，包含 "Average" 行。
    """
    # 1. 检查并转换列数据类型
    numeric_cols = df_stats.select_dtypes(include=["object"]).columns
    for col in numeric_cols:
        try:
            df_stats[col] = pd.to_numeric(df_stats[col], errors="coerce")
        except Exception as e:
            print(f"转换列 {col} 出错: {e}")

    # 2. 删除全空列
    df_stats = df_stats.dropna(axis=1, how="all")

    # 3. 重新计算数值列的平均值
    numeric_cols = df_stats.select_dtypes(include=["number"]).columns
    avg_row = df_stats[numeric_cols].mean(axis=0, skipna=True)

    # 4. 填充非数值列默认值
    avg_row["start_date"] = "N/A"
    avg_row["end_date"] = "N/A"

    # 5. 将平均值作为新行添加
    avg_row.name = "Average"
    df_stats = pd.concat([df_stats, avg_row.to_frame().T], axis=0)
    print(df_stats)

    # 6. 保存为csv 
    folder_path = os.path.join(os.path.abspath(os.getcwd()), "analysis_results")
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    file_path = os.path.join(folder_path, "statistics_results.csv")
    df_stats.to_csv(file_path, index=True, encoding="utf-8-sig")
    print(f"Statistics results have been saved to: {file_path}")

    return df_stats