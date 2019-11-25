# -*- coding: utf-8 -*-
"""
Created on Fri Oct 25 23:20:12 2019

@author: David
"""

import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import threading
import time

address_df = pd.read_csv("data/missing_labels.csv", index_col=False)
address_list = np.array_split(address_df, 200)
wallet_list = []
    
    
def scrape_owner(address_df): 
    for index, row in address_df.iterrows():
        address = row['address']        
        try:         
            url = "https://bitinfocharts.com/bitcoin/address/" + address              
            res = requests.get(url)    
            soup = BeautifulSoup(res.content,'lxml')
            
            table = soup.find_all('table')[1]
            df = pd.read_html(str(table))[0]
            owner = df.iloc[0,3]
            owner = owner.replace('wallet:', ' ').strip()
            
            wallet_list.append([address, owner])
            print("Appended wallet " + str(len(wallet_list)))
            time.sleep(2)
        except:
            print("Error")
            print(url)


for counter, address_df in enumerate(address_list):     
    print("--THREAD " + str(counter) + " STARTED")  
    thread_scrape_owner = threading.Thread(target=scrape_owner, args=(address_df))
    thread_scrape_owner.start()      


#Export to csv
wallets = pd.DataFrame(wallet_list, columns = ['address', 'owner']) 
wallets['category'] = 'Exchange' 
wallets.to_csv('found_full.csv', index = False)

def remove_digits(address): 
    numbers = sum(c.isdigit() for c in address)
    if numbers <= 2:
        return address.strip()
    else: 
        return "unknown"

wallets["owner"] = wallets["owner"].apply(remove_digits)
wallets = wallets[wallets['owner'] != 'unknown']
wallets.to_csv('wallets_bitinfocharts_missing.csv', index = False)
