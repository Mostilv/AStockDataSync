# factor_manager/factors/technical_factors.py

import talib
import numpy as np

def sma_factor(data: dict, period: int = 20) -> float:
    """
    简单移动平均因子
    :param data: dict, 包含 "close"
    :param period: SMA 周期
    :return: SMA 整条序列
    """
    close = data["close"]
    sma_array = talib.SMA(close, timeperiod=period)
    return float(sma_array[-1])

def factor_rsi_np(close_array: np.ndarray, timeperiod: int = 14) -> float:
    """
    使用talib计算RSI，直接基于numpy数组 (close_array)。
    :param close_array: 收盘价的numpy数组，长度 >= timeperiod
    :param timeperiod: RSI周期
    :return: 返回最后一个bar对应的RSI值(float)，若数据不足或异常则返回None
    """
    if len(close_array) < timeperiod:
        
        return None

    rsi_series = talib.RSI(close_array, timeperiod=timeperiod)
    if len(rsi_series) == 0:
        return None

    return float(rsi_series[-1])

def factor_macd_np(close_array: np.ndarray,
                   fastperiod: int = 12,
                   slowperiod: int = 26,
                   signalperiod: int = 9) -> float:
    """
    使用talib计算MACD，直接基于numpy数组 (close_array)。
    :return: 返回最后一个bar对应的MACD柱值 (macd_hist)；若数据不足则返回None。
    """
    min_len = max(fastperiod, slowperiod, signalperiod)
    if len(close_array) < min_len:
        return None

    macd_diff, macd_dea, macd_hist = talib.MACD(
        close_array,
        fastperiod=fastperiod,
        slowperiod=slowperiod,
        signalperiod=signalperiod
    )
    if len(macd_hist) == 0:
        return None

    return float(macd_hist[-1])



TECHNICAL_FACTORS = {
    "SMA":sma_factor,
    "RSI": factor_rsi_np,
    "MACD": factor_macd_np,
    
}
