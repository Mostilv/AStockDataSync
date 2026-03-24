import logging
import sys
import traceback
from src.data.manager_backend import StockMiddlePlatformBackendSync
import json

logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)

try:
    sync = StockMiddlePlatformBackendSync()
    sync._ensure_authenticated()
    doc = {'code': '000001', 'code_name': 'Test Stock', 'source': 'akshare', 'temporary': False}
    payload = {
        'target': sync.basic_target, 
        'provider': sync.provider, 
        sync.basic_payload_key: [sync._transform_basic_doc(doc)], 
        **sync.extra_basic_payload
    }
    url = sync._build_url(sync.basic_path)
    print(f'POSTing to {url}')
    resp = sync.session.post(url, json=payload, timeout=5)
    print(f'Status: {resp.status_code}')
    print(f'Response: {resp.text}')
except Exception as e:
    print('Error:', e)
    traceback.print_exc()
