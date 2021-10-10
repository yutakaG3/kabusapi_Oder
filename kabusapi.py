import kabusapi

import yaml
import os
import pickle
from datetime import datetime, timedelta, timezone

with open('auth.yaml', 'r') as yml:
    auth = yaml.safe_load(yml)
    url = auth['host']
    port = auth['port']
    password = auth['pass']

api = kabusapi.Context(url, port, password)

pkl_path = 'pkl'

JST = timezone(timedelta(hours=+9), 'JST')
now = datetime.now(JST)
date = "{}{:02}{:02}".format(
    str(now.year)[2:4], now.month, now.day,
)

@api.websocket
def recieve(msg):
    symbol = msg['Symbol']
    path = pkl_path + '/' + date + '/' + symbol
    ts = str(datetime.now().timestamp())

    if not os.path.exists(path):
        os.makedirs(path)
    with open(path + '/' + ts, 'wb') as f:
        pickle.dump(msg, f)

    print(symbol, msg['SymbolName'],
            msg['CurrentPrice'], msg['TradingVolumeTime'])

def symbols_register(api, symbols):
    symbol_list = []
    for symbol in symbols:  # 東証に登録
        symbol_list += [ {'Symbol': symbol, "Exchange": 1, }, ]
    data = { "Symbols":  symbol_list  } 
    return api.register(**data)

def main():
    # TOPIX Core 30 @ 201102
    symbols = [
        3382, 4063, 4452, 4502, 4503, 
        4568, 6098, 6367, 6501, 6594, 
        6758, 6861, 6954, 6981, 7203, 
        7267, 7741, 7974, 8001, 8031, 
        8058, 8306, 8316, 8411, 8766, 
        9022, 9432, 9433, 9437, 9984, 
    ]

    api.unregister.all()
    response = symbols_register(api, symbols)
    print(response)

    try:
        api.websocket.run()
    except KeyboardInterrupt:
        exit()

if __name__ == '__main__':
    main()
