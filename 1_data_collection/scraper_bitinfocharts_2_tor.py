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
from itertools import cycle
import random

# =============================================================================
# Proxies
# =============================================================================
proxies = pd.read_csv('proxies.txt', names=['ips'], header=None, index_col=False) #https://proxyscrape.com/free-proxy-list
proxies =  proxies.ips.values.tolist()
random.shuffle(proxies)
proxies_used = []

def find_working_proxy(url):
    proxy_pool = cycle(proxies)   
    found_proxy = False   
    
    while found_proxy == False: 
            test_proxy = next(proxy_pool)
            if test_proxy not in proxies_used:
                proxies_used.append(test_proxy)   
                print(len(proxies_used), "/" ,len(proxies), " proxies used", sep ="")
                try:     
                    res = requests.get(url, proxies={"http": test_proxy, "https": test_proxy})    
                    print(test_proxy + " connected")
                    found_proxy = True
                    return res, test_proxy
                except:
                    print(test_proxy + " not working. Skipping")                       


def get_response(proxy, url):
    found_proxy = False
    while found_proxy == False:    
        try:                   
            res = requests.get(url, proxies={"http": proxy, "https": proxy}) 
            found_proxy = True
            return res, proxy
        except:
            print("connection error")
            res, proxy = find_working_proxy(url)
            return res, proxy    



# =============================================================================
# Scraper
# =============================================================================
            
address_df = pd.read_csv("data/unkown_wallets_2.csv", index_col=False)
address_list = np.array_split(address_df, 100)
wallet_list = []
    
def scrape_owner(df, proxy): 
    for index, row in df.iterrows():
        address = row['address']   
        
        try:         
            
            url = "https://bitinfocharts.com/bitcoin/address/" + address              
            res, proxy = get_response(proxy, url)
            #res = requests.get(url)    
            soup = BeautifulSoup(res.content,'lxml')
            
            table = soup.find_all('table')[1]
            df = pd.read_html(str(table))[0]
            owner = df.iloc[0,3]
            owner = owner.replace('wallet:', ' ').strip()
            
            wallet_list.append([address, owner])
            print("Appended wallet " + str(len(wallet_list)))
            #time.sleep(2)
        except:
            print("Error:", url, sep=" ")
            
    print(">>>finished<<<")
    proxies_used.remove(proxy)
            

for counter, df in enumerate(address_list):   
    res, proxy = find_working_proxy("https://bitinfocharts.com/bitcoin/")
    print("--THREAD " + str(counter) + " STARTED")  
    thread_scrape_owner = threading.Thread(target=scrape_owner, args=(df, proxy))
    thread_scrape_owner.start()      



# =============================================================================
# Export to csv
# =============================================================================
wallets = pd.DataFrame(wallet_list, columns = ['address', 'owner']) 
wallets['category'] = 'Exchange' 
wallets.to_csv('wallets_bitinfocharts_with_numbers.csv', index = False)

def remove_digits(address): 
    numbers = sum(c.isdigit() for c in address)
    if numbers <= 2:
        return address.strip()
    else: 
        return "unknown"

wallets["owner"] = wallets["owner"].apply(remove_digits)
wallets = wallets[wallets['owner'] != 'unknown']
wallets.to_csv('wallets_bitinfochart_no_numbers.csv', index = False)
