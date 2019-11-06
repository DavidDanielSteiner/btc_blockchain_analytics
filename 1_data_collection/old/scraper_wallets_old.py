# -*- coding: utf-8 -*-
"""
Created on Sun Nov  3 01:22:22 2019

@author: David


https://scrapestack.com/documentation
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import threading
import time
import traceback
from lxml.html import fromstring
from itertools import cycle

#run = True
#run = False
proxy = ''
connected = False
df_all_wallets = pd.DataFrame()

df_all_wallets.reset_index()
df_all_wallets.to_csv('../data/wallet_explorer_x_x.csv', index=False)


##categories_names = ['Exchange', 'Pools', 'Services', 'Gambling', 'Historic']
categories_names = ['Exchange']


def get_proxies():
    url = 'https://free-proxy-list.net/'
    response = requests.get(url)
    parser = fromstring(response.text)
    proxies = set()
    for i in parser.xpath('//tbody/tr')[:100]:
        if i.xpath('.//td[7][contains(text(),"yes")]'):
            #Grabbing IP and corresponding PORT
            proxy = ":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
            proxies.add(proxy)
    return proxies


def scrape_owner(owner, category_name):   
    global proxy
    global connected
    print("--NEW THREAD STARTED: " + owner + "--")     
    url = "https://www.walletexplorer.com/wallet/" + owner + "/addresses?page="
    df_owner_wallets = pd.DataFrame()    
    t = threading.currentThread()
    
    #GET max_page for wallet owner
    '''  
    found_proxy = False
    while found_proxy == False:    
        try:                   
            url_pages = "https://www.walletexplorer.com/wallet/" + owner
            res = requests.get(url_pages, proxies={"http": proxy, "https": proxy}) 
            found_proxy = True
            connected = True
        except:
            connected = False
            find_working_proxy(url_pages)   
    res = requests.get(url_pages) 
    soup = BeautifulSoup(res.content,'lxml')      
    tmp = []   
    for data in soup.find_all('div', class_='paging'):
        for a in data.find_all('a'):
            tmp.append(a.get('href'))         
    tmp = tmp[-1]
    max_page = re.findall(r'\d+', tmp)[0]
    print(max_page)
    '''

    try:           
        for page in range(40,80):
            if getattr(t, "loop", True):#    
                full_url = url + str(page)
                
                found_proxy = False
                while found_proxy == False:    
                    try:                   
                        res = requests.get(full_url, proxies={"http": proxy, "https": proxy}) 
                        found_proxy = True
                        connected = True
                    except:
                        connected = False
                        find_working_proxy(full_url)
                                  
                soup = BeautifulSoup(res.content,'lxml')
                table = soup.find_all('table')[0]
                df = pd.read_html(str(table))[0]
                df['owner'] = owner
                df['category'] = category_name
                df_owner_wallets = df_owner_wallets.append(df)
                print("appended page: ",str(page), owner, category_name, sep=" ")
                time.sleep(20)

    except Exception :
        t.loop = False
        thread_scrape_owner.exit()
        print("--------", str(page), owner, "--------", sep=" ")
        print(traceback.format_exc())
        pass
    
    global df_all_wallets
    df_all_wallets = df_all_wallets.append(df_owner_wallets)
    print("Current size of dataframe: " + str(len(df_all_wallets)))
 
  
def find_working_proxy(url):
    global proxy
    if connected == False:
        proxies = get_proxies()
        proxy_pool = cycle(proxies)   
        proxy = next(proxy_pool)
        found_proxy = False
    
        while found_proxy == False: 
            if connected == False:
                test_proxy = next(proxy_pool)
                try:
                    res = requests.get(url,proxies={"http": test_proxy, "https": test_proxy})                 
                    print(proxy + " connected")
                    proxy = test_proxy
                    found_proxy = True
                    #return proxy
                except:
                    #print(traceback.format_exc())
                    print(proxy + " connection error. Skipping")            

                        
                        
    
# =============================================================================
# START    
# =============================================================================
    
print('Searching for connection. Please wait')   
found_proxy = False
while found_proxy == False:    
    try: 
        res = requests.get("https://www.walletexplorer.com/",proxies={"http": proxy, "https": proxy}) 
        found_proxy = True
        connected = True
    except:
        connected = False
        find_working_proxy("https://www.walletexplorer.com/")
  
#res = requests.get("https://www.walletexplorer.com/")
soup = BeautifulSoup(res.content,'lxml')
categories_lists = soup.find_all('ul')   


for counter, category in enumerate(categories_names):
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
        time.sleep(10)

            
        
 
