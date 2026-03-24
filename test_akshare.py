import os
os.environ["http_proxy"] = ""
os.environ["https_proxy"] = ""
os.environ["HTTP_PROXY"] = ""
os.environ["HTTPS_PROXY"] = ""

import socket
from requests.packages.urllib3.util import connection
connection.allowed_gai_family = lambda: socket.AF_INET

import akshare as ak

print("Calling spot...")
df = ak.stock_zh_a_spot_em()
print(df.columns.tolist())

print("Calling hist...")
df2 = ak.stock_zh_a_hist(symbol="000001", period="daily", start_date="20230101", end_date="20230105", adjust="qfq")
print(df2.columns.tolist())

print("Calling hist min...")
df3 = ak.stock_zh_a_hist_min_em(symbol="000001", start_date="2024-03-01 09:30:00", end_date="2024-03-01 15:00:00", period="5", adjust="qfq")
print(df3.columns.tolist())
