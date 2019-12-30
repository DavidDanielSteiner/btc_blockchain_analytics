# -*- coding: utf-8 -*-
"""
Created on Tue Dec 23 18:40:24 2019

@author: David
"""

import pandas as pd
import dask.dataframe as dd    
import io
import requests

# =============================================================================
# Merge Data
# =============================================================================
from data_merging import merge_data, filter_data, get_unknown_wallets

#Import data sources
response=requests.get('https://coinmetrics.io/newdata/btc.csv').content
btc_price_data=pd.read_csv(io.StringIO(response.decode('utf-8'))) #https://coinmetrics.io/community-data-dictionary/
transactions = pd.read_csv("../data/transactions_100BTC.csv")
wallets = pd.read_csv("../data/btc_wallets.csv")

#Combine all data sources
tnx = merge_data(btc_price_data, transactions, wallets)

#filtered_tnx = filter_data(data, filter_type = 'dollar', value=100000)
#filtered_tnx.to_csv("transactions_filtered_100K.csv", index=False)
#wallets.to_csv("wallets.csv", index=False)
#unknown_wallets = get_unknown_wallets(filtered_tnx)
#unknown_wallets.to_csv(" .csv", index = False)

# =============================================================================
# Group Transactions
# =============================================================================
from common_input_clustering import merge_tnx_wallets, group_transactions, regroup, add_category
from data_merging import add_new_wallets

#wallets = pd.read_csv("../data/btc_wallets.csv", index_col=False)  
#tnx = pd.read_csv("../data/transactions_filtered_10MIO.csv", index_col=False)  
tnx = tnx.drop(['sender_name', 'sender_category', 'receiver_name', 'receiver_category', 'CapMrktCurUSD'], axis=1)  
#tnx = dd.from_pandas(tmp, npartitions=10)

#label all addresses within same transaction hash, if one address is labeled 
labeled_wallets = pd.DataFrame()
#labeled_wallets = dd.from_pandas(labeled_wallets, npartitions=10)

for i in range(10):
    labeled_tnx = merge_tnx_wallets(tnx, wallets, labeled_wallets)
    df_grouped = group_transactions(labeled_tnx)
    labeled_wallets = labeled_wallets.append(regroup(df_grouped)).drop_duplicates(keep='last')
    print(len(labeled_wallets))

#Add cateogory to owners
labeled_tnx = add_category(wallets, labeled_tnx)

#Add new addresses to btc_wallets
btc_wallets_new = add_new_wallets(wallets, labeled_wallets)
btc_wallets_new.to_csv("btc_wallets_new.csv", index=False)

#Filter
#filter_name, filtered_tnx = filter_data(labeled_tnx, filter_type = 'dollar', value=100000)
filter_name, filtered_tnx = filter_data(labeled_tnx, filter_type = 'marketcap', value=0.01)
#filtered_transactions = labeled_tnx[labeled_tnx['dollar'] >= 10000000]

#Export
filtered_tnx.to_csv("transactions_" + filter_name + ".csv", index=False)


# =============================================================================
# Get list of unknown addresses  (for scraping)
# =============================================================================
from data_merging import filter_data

unknown_addresses = get_unknown_wallets(filtered_tnx)
filtered_tnx.to_csv("unknown_addresses.csv", index=False)


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






# =============================================================================
# Random
# =============================================================================


##exchangedata
df = pd.read_csv("data/transactions_filtered_10MIO.csv")
df['percent_marketcap'].mean()
#tmp = df[df['percent_marketcap'] >= 0.01]
#x = tmp.drop_duplicates(subset='hash')
#tmp['dollar'].median()

#get unique addresses
sender = df[['sender', 'sender_category']]
sender.rename(columns = {"sender" : 'address', 'sender_category': 'class'}, inplace = True) 
receiver = df[['receiver', 'receiver_category']]
receiver.rename(columns = {"receiver" : 'address', 'receiver_category' : 'class'}, inplace = True) 
labels = sender.append(receiver)
labels = labels.drop_duplicates(subset='address', keep='last')    
#tmp = labels.drop_duplicates(subset='class')
#df = labels[labels['class'] == 'Exchange']
#df = labels[labels['class'].isnull()]


#Export Data
category = offchain[['address', 'class']]
df_features_2 = pd.merge(df_features,category,on='address',how='inner')
df_features = df_features.drop(['address'], axis = 1)     
df_features_2.to_csv("testdata_10k_features.csv", index=False)    
    


