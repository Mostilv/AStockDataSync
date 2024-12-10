# dataprep/converter.py
from typing import List
import pandas as pd
from vnpy.trader.object import BarData, Exchange
from vnpy.trader.constant import Interval  # 导入Interval枚举

def convert_to_vnpy_format(df: pd.DataFrame, ts_code: str) -> List[BarData]:
    """
    将清洗后的DataFrame转换为vn.py可识别的BarData列表。
    """
    symbol, exchange_str = ts_code.split(".")

    if "SH" in exchange_str.upper():
        exchange = Exchange.SSE
    elif "SZ" in exchange_str.upper():
        exchange = Exchange.SZSE
    else:
        exchange = Exchange.SSE  # 默认SSE

    bars = []
    for _, row in df.iterrows():
        bar = BarData(
            symbol=symbol.strip(),
            exchange=exchange,
            datetime=row["trade_date"].to_pydatetime(),
            interval=Interval.DAILY,  # 修正为枚举类型
            open_price=float(row["open"]),
            high_price=float(row["high"]),
            low_price=float(row["low"]),
            close_price=float(row["close"]),
            volume=float(row["vol"]),
            turnover=float(row["amount"]),
            open_interest=0,
            gateway_name=""
        )
        bars.append(bar)
    return bars
