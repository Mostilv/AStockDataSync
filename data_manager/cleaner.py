# dataprep/cleaner.py
import pandas as pd

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    数据清洗：
    1. 日期字段转换为datetime
    2. 按交易日期排序
    3. 缺失值处理（前向后向填充）
    4. 简单异常值过滤（如open<=0的数据行）
    """
    # 转换日期
    df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
    df = df.sort_values('trade_date')
    # 缺失值填充
    df = df.ffill().bfill()
    # 移除异常数据行
    df = df[df['open'] > 0]

    return df
