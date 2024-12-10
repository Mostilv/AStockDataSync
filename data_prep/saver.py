# dataprep/saver.py
from typing import List
from vnpy.trader.object import BarData
from vnpy.trader.database import get_database

def save_data_to_vnpy_db(bars: List[BarData]):
    """
    使用VN.PY的database_manager接口将BarData保存到数据库中。
    具体使用哪个数据库取决于database.json的配置（此处为MongoDB）。
    """
    database = get_database()
    print(database)
    result = database.save_bar_data(bars)
    

    if result:
        print(f"{len(bars)} bars have been saved to the VN.PY database.")
    else:
        print("Failed to save bars to the VN.PY database.")
