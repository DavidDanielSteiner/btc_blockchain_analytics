# -*- coding: utf-8 -*-
"""
Created on Tue Dec 23 18:40:24 2019

@author: David
"""

import pandas as pd
import dask.dataframe as dd    


# =============================================================================
# Merge Data
# =============================================================================




# =============================================================================
# Group Transactions and get unknown addresses
# =============================================================================




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
    


