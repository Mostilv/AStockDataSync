import os
os.environ["PYTHONIOENCODING"] = "utf-8"

from vnpy.trader.setting import SETTINGS

from datetime import datetime
from data_manager.manager_baostock import BaostockManager
from data_manager.vnpy_db import VnpyDBManager


SETTINGS["database.name"] = "mongodb"
SETTINGS["database.database"] = "vnpy"
SETTINGS["database.host"] = "127.0.0.1"
SETTINGS["database.port"] = 27017

def main():

    
    print('func main')

def manage_baostock_data():
    bs_manager = BaostockManager()
    # bs_manager.get_stock_basic_info()
    bs_manager.update_stocks_daily()
    bs_manager.close()

def manage_vnpy_data():
    vnpy_db_manager = VnpyDBManager()
    vnpy_db_manager.convert_all_baostock_to_vnpy()
    vnpy_db_manager.close()
    
if __name__ == "__main__":
    # manage_baostock_data()
    # manage_vnpy_data()
    main()
    