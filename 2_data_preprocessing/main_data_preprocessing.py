# -*- coding: utf-8 -*-
"""
Created on Tue Dec 23 18:40:24 2019

@author: David
"""

import pandas as pd
import io
import requests

# =============================================================================
# Merge Data
# =============================================================================
from data_merging import merge_data, filter_data

#Import data sources
response=requests.get('https://coinmetrics.io/newdata/btc.csv').content
btc_price_data=pd.read_csv(io.StringIO(response.decode('utf-8'))) #https://coinmetrics.io/community-data-dictionary/
transactions = pd.read_csv("../data/transactions_100BTC.csv")
wallets = pd.read_csv("../data/btc_wallets.csv")

#Combine all data sources
tnx = merge_data(btc_price_data, transactions, wallets)
tnx.to_csv("transactions_100BTC_merged.csv", index=False)


# =============================================================================
# Group Transactions
# =============================================================================
from common_input_clustering import merge_tnx_wallets, group_transactions, regroup, add_category
from data_merging import add_new_wallets

tnx = pd.read_csv("../data/transactions_100BTC_merged.csv", index_col=False)  
tnx = tnx.drop(['sender_name', 'sender_category', 'receiver_name', 'receiver_category', 'CapMrktCurUSD'], axis=1)  

#label all addresses within same transaction hash, if one address is labeled 
labeled_wallets = pd.DataFrame()

for i in range(5):
    labeled_tnx = merge_tnx_wallets(tnx, wallets, labeled_wallets)
    df_grouped = group_transactions(labeled_tnx)
    labeled_wallets = labeled_wallets.append(regroup(df_grouped)).drop_duplicates(keep='last')
    print(len(labeled_wallets))


#Add cateogory to owners
labeled_tnx = add_category(wallets, labeled_tnx)
labeled_tnx.to_csv("transactions_100BTC_labeled.csv", index=False)

#Add new addresses to btc_wallets
btc_wallets_new = add_new_wallets(wallets, labeled_wallets)
btc_wallets_new.to_csv("btc_wallets_new.csv", index=False)


#Filter
#filter_name, filtered_tnx = filter_data(labeled_tnx, filter_type = 'dollar', value=100000)
filter_name, filtered_tnx = filter_data(labeled_tnx, filter_type = 'marketcap', value=0.01)

#filtered_tnx = pd.read_csv("../data/transactions_0.01_marketcap.csv")


#Select transactions after 2015
filtered_tnx['block_timestamp'] = pd.to_datetime(filtered_tnx['block_timestamp']) 
filtered_tnx = filtered_tnx[filtered_tnx['block_timestamp'].dt.year >= 2015]
filtered_tnx.to_csv("transactions_" + filter_name + ".csv", index=False)

# =============================================================================
# Get list of unknown addresses (for scraping)
# =============================================================================
from data_merging import get_unknown_wallets

unknown_addresses, known_addresses = get_unknown_wallets(filtered_tnx)
unknown_addresses.to_csv("addresses_unknown.csv", index=False)
known_addresses = pd.merge(wallets, known_addresses, how='inner', on='address')
known_addresses.to_csv("addresses_known.csv", index=False)

# =============================================================================
# Feature engineering
# =============================================================================
from feature_engineering_dask_v2 import get_features

#trainingsdata testsample 7 classes
#all_tnx = dd.read_csv("data/testdata_30k.csv")
#tmp = all_tnx.drop_duplicates(subset='address').compute()
#tmp = tmp.sample(n=10000, random_state = 1)['address']
#all_tnx = dd.merge(all_tnx,tmp,on='address',how='inner')

#Exhange dataset
#all_tnx = dd.read_csv("data/testdata_5k_exchange.csv")

category_names = ['Exchange','Gambling','Service','Mixer','Mining']
features_all_categories = pd.DataFrame()  
category_name = 'Mining'

for category_name in category_names:
    #Import csv with adr details
    all_tnx = pd.read_csv("../data/address_" + category_name + ".csv")
    
    #create new features
    df_features = get_features(all_tnx)
    
    #append 
    features_all_categories = features_all_categories.append(df_features)
    
    #Export csv with features
    df_features.to_csv("features_" + category_name + ".csv", index=False)  
    print(category_name, "exported", sep=" ")
  
#Export all features
features_all_categories.to_csv("features_all_categories.csv", index=False) 




