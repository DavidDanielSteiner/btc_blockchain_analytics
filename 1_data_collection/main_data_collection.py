# -*- coding: utf-8 -*-
"""
Created on Tue Dec 24 16:38:40 2019

@author: David
"""

import pandas as pd



# =============================================================================
# Scrape data
# =============================================================================


# =============================================================================
# Create transactions dataset
# =============================================================================
from bigquery_BTC_node import get_all_tx_over_value

btc=50
large_tx = get_all_tx_over_value(btc)
large_tx.to_csv("transactions_" + str(btc) +'BTC.csv", index=False)

# =============================================================================
# Create training and testset for address classification
# =============================================================================
from bigquery_BTC_node import get_all_tx_from_address

df = pd.read_csv("../data/btc_wallets.csv")

exchange = df[df['category'] == 'Exchange'].sample(n=10000, random_state = 1)['address'].to_list()
gambling = df[df['category'] == 'Gambling'].sample(n=10000, random_state = 1)['address'].to_list()
service = df[df['category'] == 'Service'].sample(n=10000, random_state = 1)['address'].to_list()
mixer = df[df['category'] == 'Mixer'].sample(n=10000, random_state = 1)['address'].to_list()
mining = df[df['category'] == 'Mining'].sample(n=10000, random_state = 1)['address'].to_list()

category_list = [exchange, gambling, service, mixer, mining]
category_names = ['Exchange','Gambling','Service','Mixer','Mining']

for address_list, category_name in zip(category_list, category_names):
    all_tx = get_all_tx_from_address(address_list)
    all_tx['category'] = category_name    
    all_tx.to_csv("address_" + category_name + ".csv", index=False)
    print(category_name, "saved to csv", sep=" ")


#category = df[['address', 'class']]
#all_tnx_class = pd.merge(all_tnx,category,on='address',how='inner')
#all_tnx_class.to_csv("testdata_5k_unknown.csv", index=False)
    
    
