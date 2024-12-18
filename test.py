import baostock as bs

lg = bs.login()
print("login respond  error_code:", lg.error_code)
print("login respond  error_msg:", lg.error_msg)

rs = bs.query_history_k_data_plus("sh.600000",
    "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,psTTM,pcfNcfTTM,pbMRQ,isST",
    start_date='2024-12-01', end_date='2024-12-31',
    frequency="d", adjustflag="3")

print("query_history_k_data_plus respond  error_code:", rs.error_code)
print("query_history_k_data_plus respond  error_msg:", rs.error_msg)

# # 显示 ResultData 对象中所有属性名和方法
print(dir(rs))
print('============================================')

# 显示 ResultData 对象的所有实例属性及对应值
print(rs.__dict__)

bs.logout()