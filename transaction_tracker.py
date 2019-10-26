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
import pandas as pd




# =============================================================================
# DB
# =============================================================================

import importlib.util
spec = importlib.util.spec_from_file_location("module.name", "C:/users/config.py")
config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(config)
DB_CREDENTIALS = config.DB_DATASTIG_CRYPTO 



import mysql.connector
from mysql.connector import Error

def connect():
    try:
        connection = mysql.connector.connect(**DB_CREDENTIALS)
        cursor = connection.cursor()       
        return cursor, connection              
    except Error as e:
        print("Error while connecting to MySQL", e)











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
        btc_value = satoshi/100000000 
        dollar_value = btc_value * btc_price
        
        if dollar_value > 100000:
            transaction_hash = result["x"]["hash"]
            sender_address = result["x"]["inputs"][0]["prev_out"]["addr"]
            receiver_address = result["x"]["out"][0]["addr"]
            timestamp = result["x"]["time"]
            date = datetime.fromtimestamp(timestamp)
            
            #print(int(dollar), sender_wallet, receiver_wallet, date, sep = "//")
            print(int(dollar_value), date, sep = " ")
            
            thread_save_to_db = threading.Thread(target=save_to_db, 
                                                 args=(btc_value, dollar_value,transaction_hash,sender_address,receiver_address,timestamp,date))
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
        

def save_to_db(btc_value, dollar_value,transaction_hash,sender_address,receiver_address,timestamp,date):
    #contains_sender = df_wallets['address'].str.contains(sender_wallet).any()
    #print("Sender " + str(contains_sender))
    
    sender = df_wallets[df_wallets['address'].str.match(sender_address)]
    try:
        sender_type = sender["type"].values[0]
        sender_name = sender["owner"].values[0]
        print(sender_type, sender_name, sep= " ")
    except:
        sender_type = "unknown"
        sender_name = "unknown"
        print("sender not in list:" + sender_address)
        
    receiver = df_wallets[df_wallets['address'].str.match(receiver_address)]
    try:
        receiver_type = receiver["type"].values[0]
        receiver_name = receiver["owner"].values[0]
        print(receiver_type, receiver_name, sep= " ")
    except:
        receiver_type = "unknown"
        receiver_name = "unknown"       
        print("receiver not in list:" + receiver_address)
        
        
    query_insert = """
    INSERT INTO btc_transactions (transaction_hash, btc_value, dollar_value, timestamp, date, sender_address, receiver_address, sender_name, receiver_name, sender_type, receiver_type)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    cur, conn = connect()
    cur.execute(query_insert, (transaction_hash, btc_value, dollar_value, timestamp, date, sender_address, receiver_address, sender_name, receiver_name, sender_type, receiver_type))
    conn.commit()
    print("Record inserted successfully")    
    cur.close()
    conn.close()


    
    


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
