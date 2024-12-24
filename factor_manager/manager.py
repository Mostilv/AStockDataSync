# factor_manager/manager.py

from typing import Dict, Any, List, Optional
import numpy as np

# 从技术因子文件导入
from .factors.technical_factors import TECHNICAL_FACTORS

class FactorManager:
    """
    简化版的因子管理器，支持直接基于 numpy 数组来计算因子。
    """

    def __init__(self):
        self.factor_dict: Dict[str, Any] = {}
        # 注册技术因子
        for name, func in TECHNICAL_FACTORS.items():
            self.factor_dict[name] = func
        # 如果有基本面因子，也可注册(此处略)
        # from .factors.fundamental_factors import FUNDAMENTAL_FACTORS
        # for name, func in FUNDAMENTAL_FACTORS.items():
        #     self.factor_dict[name] = func

    def calculate_factors(self, 
                          close_array: np.ndarray,
                          factor_names: Optional[List[str]] = None
                          ) -> Dict[str, float]:
        """
        基于收盘价数组(or 其他数组)计算指定因子。只计算最新值
        :param close_array: 收盘价序列。
        :param factor_names: 要计算的因子名；None表示计算全部
        :return: {因子名称: 因子值} 
        """
        if factor_names is None:
            factor_names = list(self.factor_dict.keys())

        result = {}
        for name in factor_names:
            func = self.factor_dict.get(name)
            if func is None:
                continue
            val = func(close_array)  # 这里直接把 close_array 传进去
            result[name] = val
        return result
    
