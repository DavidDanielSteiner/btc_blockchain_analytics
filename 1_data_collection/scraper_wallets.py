# -*- coding: utf-8 -*-
"""
Created on Sun Nov  3 01:22:22 2019

@author: David
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import threading
import time

df_all_wallets = pd.DataFrame()
df_all_wallets.reset_index()
categories_names = ['Exchange', 'Pools', 'Services', 'Gambling', 'Historic']
#categories_names = ['Exchange']

def scrape_owner(owner, category_name):        
    
    for owner in owners_list:
        url = "https://www.walletexplorer.com/wallet/" + owner + "/addresses?page="
        df_owner_wallets = pd.DataFrame()
        
        try:           
            for page in range(1,1000):            
                res = requests.get(url + str(page))
                soup = BeautifulSoup(res.content,'lxml')
                table = soup.find_all('table')[0]
                df = pd.read_html(str(table))[0]
                df['owner'] = owner
                df['category'] = category_name
                df_owner_wallets = df_owner_wallets.append(df)
                print("appended page: ",str(page), "for owner", owner, "and category", category_name, sep=" ")
                time.sleep(1)
        except:
            pass
        
        global df_all_wallets
        df_all_wallets = df_all_wallets.append(df_owner_wallets)
        print(df_all_wallets)
 
    
    
res = requests.get("https://www.walletexplorer.com/")
soup = BeautifulSoup(res.content,'lxml')
categories_lists = soup.find_all('ul')   

for counter, category in enumerate(categories_names):
    print(counter)
    category_name = categories_names[counter]
    categories_list = categories_lists[counter]
   
    owners_list = []
    for litag in categories_list.find_all('li'):
        pattern = r'\(.*?\)'
        owner = litag.text
        owner = re.sub(pattern, '', owner).strip()
        owners_list.append(owner)
        
    for owner in owners_list:
        thread_scrape_owner = threading.Thread(target=scrape_owner, args=(owner, category_name))
        thread_scrape_owner.start()
        
        
        

        
    