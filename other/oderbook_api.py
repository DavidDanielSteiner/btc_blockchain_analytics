# -*- coding: utf-8 -*-
"""
Created on Sun Oct 27 12:05:08 2019

@author: David
"""

import requests
import json

URL = 'https://dev-api.shrimpy.io/v1/orderbooks?exchange=all&baseSymbol=BTC&quoteSymbol=USDT&limit=10'
all_exchanges = requests.get(URL).json()
all_exchanges = all_exchanges[0]["orderBooks"]

all_exchanges_list = []
for exchange in all_exchanges:
    ex = exchange['exchange']
    all_exchanges_list.append(ex)


exchange = all_exchanges_list[0]
URL = 'https://dev-api.shrimpy.io/v1/orderbooks?exchange=' + exchange + '&baseSymbol=BTC&quoteSymbol=USDT&limit=1000'
orderbook = requests.get(URL).json()



