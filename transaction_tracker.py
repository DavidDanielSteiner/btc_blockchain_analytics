# -*- coding: utf-8 -*-
"""
Created on Thu Oct 24 23:06:50 2019

@author: David

https://www.blockchain.com/explorer?currency=BTC&stat=transactions
"""


import json
import requests
from websocket import create_connection
from datetime import datetime
import time
import threading


ws = create_connection("wss://ws.blockchain.info/inv")
ws.send(json.dumps({"op":"unconfirmed_sub"}))

btc_price = 0
run = True
#run = False
#ws.close()

df_wallets = pd.read_csv('wallets.csv')



def run_transactions():
    while run == True:
        result = ws.recv()
        result = json.loads(result)
        
        satoshi = result["x"]["inputs"][0]["prev_out"]["value"]
        btc = satoshi/100000000 
        dollar = btc * btc_price
        
        if dollar > 10000:
            sender_wallet = result["x"]["inputs"][0]["prev_out"]["addr"]
            receiver_wallet = result["x"]["out"][0]["addr"]
            timestamp = result["x"]["time"]
            date = datetime.fromtimestamp(timestamp)
            
            print(int(dollar), sender_wallet, receiver_wallet, date, sep = "//")
            
            thread_save_to_db = threading.Thread(target=save_to_db, args=(timestamp,))
            thread_save_to_db.start()
            


def run_btc_price_ticker():
    while run == True:
        URL = 'https://www.bitstamp.net/api/ticker/'
        try:
            r = requests.get(URL)
            global btc_price 
            btc_price = float(json.loads(r.text)['last'])
            print("----------" + str(btc_price))
        except requests.ConnectionError:
            print("Error querying Bitstamp API")
            
        time.sleep(60)
        

def save_to_db(timestamp):
    print("save")
    
    


thread_btc_price_ticker = threading.Thread(target=run_btc_price_ticker)
thread_transactions = threading.Thread(target=run_transactions)

thread_btc_price_ticker.start()
thread_transactions.start()





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
