# -*- coding: utf-8 -*-
"""
Created on Fri Oct 25 23:20:12 2019

@author: David
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import re


df_btc_wallets = pd.DataFrame()


res = requests.get("https://bitinfocharts.com/top-100-richest-bitcoin-addresses.html")
soup = BeautifulSoup(res.content,'lxml')

'''Page 1'''
table = soup.find_all('table')[2] 
df = pd.read_html(str(table))
df_top = df[0]
#header = list(df_top.columns.values)
header = ['ranking', 'address_full', 'balance', 'percentage_coins', 'first_in', 'last_in', 'number_ins', 'first_out', 'last_out', 'number_outs']
df_top.columns = header

#print(df[0].to_json(orient='records'))
table = soup.find_all('table')[3] 
df = pd.read_html(str(table))
df_bottom = df[0]
df_bottom.columns = header


df_btc_wallets = df_btc_wallets.append(df_top)
df_btc_wallets = df_btc_wallets.append(df_bottom)


'''Page 2ff'''
for page in range(2, 101):
    print("Appended page: " + str(page))
    
    res = requests.get("https://bitinfocharts.com/top-100-richest-bitcoin-addresses-" + str(page) + ".html")
    soup = BeautifulSoup(res.content,'lxml')
    
    table = soup.find_all('table')[0] 
    df = pd.read_html(str(table))
    df_top = df[0]
    df_top.columns = header
    
    table = soup.find_all('table')[1] 
    df = pd.read_html(str(table))
    df_bottom = df[0]
    df_bottom.columns = header
    
    df_btc_wallets = df_btc_wallets.append(df_top)
    df_btc_wallets = df_btc_wallets.append(df_bottom)

#Export to csv
df_btc_wallets.to_csv('top_10000_btc_wallets.csv', index = False)


"""Data Cleaning"""
#df = df_btc_wallets
df = pd.read_csv('top_10000_btc_wallets.csv')

def get_owner(address_full):
    address_full.replace('  ', ' ')
    wallet_owner = address_full.split(' ')[-1]    
    numbers = sum(c.isdigit() for c in wallet_owner)
    length = len(wallet_owner)    
    if numbers <= 1 and length < 25:
        return wallet_owner.strip()
    else: 
        return "unknown"


def check_wallet_type(row):
    '''
    EXCHANGE = more than 200 ins
    WHALE = more than 10.000 Bitcoin and less than 200 ins
    BIG_FISH = less than 10.000 Bitcoin and less than 200 ins
    '''
    
    if row['owner'] != "unknown":
        return "EXCHANGE"
    elif row['number_ins'] >= 200:
        return "EXCHANGE"
    elif row['balance'] >= 10000:
        return "WHALE"
    else:
        return "BIG_FISH"


df["address_full"] = df["address_full"].apply(lambda x: x.replace('wallet:', ' '))
df["owner"] = df["address_full"].apply(get_owner)
df["owner"] = df["address_full"].apply(get_owner)
df["address"] = df["address_full"].apply(lambda x: x.split(' ')[0])

df["balance"] = df["balance"].apply(lambda x: x.split(' ')[0])
df["balance"] = df["balance"].replace(regex=[','], value='')
df["balance"] = pd.to_numeric(df["balance"])

#categorize wallets
df['type'] = df.apply(check_wallet_type, axis=1)

df = df[['address', 'owner', 'type']]
df.to_csv('wallets.csv', index = False)



#TODO find patterns from known exchange with clustering and ml
# CHECK why some wallets have no outs
