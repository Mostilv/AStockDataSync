# factor_manager/factors/fundamental_factors.py

import pandas as pd

def factor_pe_ratio(df: pd.DataFrame):
    """
    假设从DataFrame中取到当前PE(市盈率)进行简单返回。
    注意：这里需要你的数据源在df_data中含有PE列，或者你在此处做数据查询
    """
    if 'pe' not in df.columns or df['pe'].isnull().any():
        return None
    
    # 返回最后一条bar对应的pe值
    return float(df['pe'].iloc[-1])

def factor_pb_ratio(df: pd.DataFrame):
    """
    假设从DataFrame中取到当前PB(市净率)进行简单返回。
    """
    if 'pb' not in df.columns or df['pb'].isnull().any():
        return None

    return float(df['pb'].iloc[-1])

# 其他更多基本面因子可继续扩展...

FUNDAMENTAL_FACTORS = {
    "PE": factor_pe_ratio,
    "PB": factor_pb_ratio,
    # 后续可继续在此处添加更多基本面因子
}
