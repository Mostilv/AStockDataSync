# dataprep/run.py
# 获取指定数据，保存到对应数据库
# 清洗数据，转换为vnpy格式，保存到vnpy数据库

from .fetcher import fetch_data
from .cleaner import clean_data
from .converter import convert_to_vnpy_format
from .saver import save_data_to_vnpy_db

def run_pipeline(ts_code: str, start_date: str, end_date: str):
    """
    一键执行流程:
    1. 获取数据
    2. 清洗数据
    3. 转换为vn.py格式
    4. 保存到数据库
    """
    df = fetch_data(ts_code, start_date, end_date)
    df = clean_data(df)
    bars = convert_to_vnpy_format(df, ts_code)
    save_data_to_vnpy_db(bars)
