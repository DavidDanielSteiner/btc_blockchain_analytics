# -*- coding: utf-8 -*-
"""
Created on Tue Dec 24 18:40:24 2019

@author: David
"""

import pandas as pd
import dask.dataframe as dd    



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

#Unknown dataset
all_tnx = dd.read_csv("data/testdata_5k_unknown.csv")

df_features = get_features(all_tnx)

#Export csv with features
df_features.to_csv("testdata_5k_features_unknown.csv", index=False)     
  








# =============================================================================
# 
# =============================================================================


#Data for sql

##testdataset
offchain = pd.read_csv("data/offchain.csv", index_col=False)
offchain = offchain.dropna(subset=['class'])
list_addresses = offchain.sample(n=5000, random_state = 42)
list_addresses = offchain['address' , 'class']
list_addresses.to_csv('address_train_5k_1.csv', index=False)

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

df = df.sample(frac=1).reset_index(drop=True)
df = df[0:5000]
df.to_csv('address_unknown_5k_1.csv', index=False)


#Export Data
category = offchain[['address', 'class']]
df_features_2 = pd.merge(df_features,category,on='address',how='inner')
df_features = df_features.drop(['address'], axis = 1)     
df_features_2.to_csv("testdata_10k_features.csv", index=False)    
    


