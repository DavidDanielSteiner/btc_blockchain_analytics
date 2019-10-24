# -*- coding: utf-8 -*-
"""
Created on Thu Oct 24 23:06:50 2019

@author: David

https://www.blockchain.com/explorer?currency=BTC&stat=transactions
"""


import json
from websocket import create_connection

ws = create_connection("wss://ws.blockchain.info/inv")
ws.send(json.dumps({"op":"unconfirmed_sub"}))


while True:
    result = ws.recv()
    result = json.loads(result)
    TMP = result["x"]["inputs"][0]["prev_out"]["value"]
    dollar = TMP/100000000 * 7000
    if dollar > 100000:
        print(round(dollar,2))
    #print ("Received '%s'" % result)

ws.close()





'''
ws = create_connection("wss://api2.bitfinex.com:3000/ws")
#ws.connect("wss://api2.bitfinex.com:3000/ws")
ws.send(json.dumps({
    "event": "subscribe",
    "channel": "book",
    "pair": "BTCUSD",
    "prec": "P0"
}))


while True:
    result = ws.recv()
    result = json.loads(result)
    print ("Received '%s'" % result)

ws.close()
'''
