# -*- coding: utf-8 -*-
"""
Created on Mon Dec 16 14:49:53 2019

@author: David
"""

import pandas as pd
import threading
import numpy as np


def feature_engineering(df):
    df = df.sort_values('block_number')    
    df = df.reset_index(drop=True)        
    
    #Calculate Balance
    for index, row in df.iterrows():
        if index == 0:
            balance = 0
        else:
            balance = df.iloc[[index - 1]]['balance_btc']
            
        if row['type'] == 'output':
            balance += row['value_btc']
        else:
            balance -= row['value_btc']
        df.at[index,'balance_btc'] = balance
     
    df['balance_usd'] = df['balance_btc'] * df['PriceUSD']
        
    #Lifetime and inputs
    tx = df.sort_values('type') 
    tx = tx.reset_index(drop=True)  
    tx = tx.drop_duplicates(subset='hash', keep='first') #keep inputs
    tx_type = df.drop_duplicates(subset=['hash', 'type'], keep='first')

    df['n_tx'] = len(tx)
    df['lifetime'] = (((max(df['block_timestamp'])) - (min(df['block_timestamp']))).days) +1
    df['tx_per_day'] = df['n_tx'] / df['lifetime']

    inputs = tx[tx['type'] == 'input']
    outputs = tx[tx['type'] == 'output']
    df['n_inputs'] = len(inputs)
    df['n_outputs'] = len(outputs)
    df['p_inputs'] = len(inputs) / df['n_tx']
    
    #transaction value usd category count 
    usd_per_tx = df[['hash', 'value_usd']].groupby('hash').agg({'value_usd':'sum'})
    df['p_0k'] = len(usd_per_tx[usd_per_tx['value_usd'] <= 100]) / df['n_tx']
    df['p_1k'] = len(usd_per_tx[ (usd_per_tx['value_usd'] >= 100) & (usd_per_tx['value_usd'] < 1000)]) / df['n_tx']
    df['p_10k'] = len(usd_per_tx[ (usd_per_tx['value_usd'] >= 1000) & (usd_per_tx['value_usd'] < 10000)]) / df['n_tx']
    df['p_100k'] = len(usd_per_tx[ (usd_per_tx['value_usd'] >= 10000) & (usd_per_tx['value_usd'] < 100000)]) / df['n_tx']
    df['p_1m'] = len(usd_per_tx[ (usd_per_tx['value_usd'] >= 100000) & (usd_per_tx['value_usd'] < 1000000)]) / df['n_tx']
    df['p_10m'] = len(usd_per_tx[ (usd_per_tx['value_usd'] >= 1000000) & (usd_per_tx['value_usd'] < 10000000)]) / df['n_tx']
    df['p_100m'] = len(usd_per_tx[ (usd_per_tx['value_usd'] >= 10000000) & (usd_per_tx['value_usd'] < 100000000)]) / df['n_tx']
    df['p_1b'] = len(usd_per_tx[ (usd_per_tx['value_usd'] >= 100000000) & (usd_per_tx['value_usd'] < 1000000000)]) / df['n_tx']
    df['p_10b'] = len(usd_per_tx[usd_per_tx['value_usd'] >= 1000000000]) / df['n_tx']
    
    #paypack rate (address in input and output)
    inputs = tx_type[tx_type['type'] == 'input']
    outputs = tx_type[tx_type['type'] == 'output']    
    payback = pd.merge(inputs,outputs, how='inner', on='hash')
    df['p_payback'] = len(payback) / df['n_tx']
    
    #Address counts per transaction
    df['mean_inputs'] = inputs['input_count'].mean()
    df['std_inputs'] = inputs['input_count'].std()
    df['mean_outputs'] = outputs['output_count'].mean()
    df['std_outputs'] = outputs['output_count'].std()
    
    #balance 
    df['mean_balance_btc'] = df['balance_btc'].mean()   
    df['std_balance_btc']  = df['balance_btc'].std() 
    df['mean_balance_usd'] = df['balance_usd'].mean()  
    df['std_balance_usd'] = df['balance_usd'].std() 
        
    #totql address input and output transaction value
    df['adr_inputs_btc'] = df[df['type'] == 'input']['value_btc'].sum()
    df['adr_outputs_btc'] = df[df['type'] == 'output']['value_btc'].sum()
    df['adr_inputs_usd'] = df[df['type'] == 'input']['value_usd'].sum()
    df['adr_outputs_usd'] = df[df['type'] == 'output']['value_usd'].sum()
    df['adr_dif_usd'] = df['adr_inputs_usd'] - df['adr_outputs_usd']
    df['p_adr_dif_usd'] = (df['adr_inputs_usd'] - df['adr_outputs_usd']) / df['adr_inputs_usd']
    
    #adr transaction value (one address)
    df['input_mean_value_btc'] = inputs['value_btc'].mean()
    df['input_std_value_btc'] = inputs['value_btc'].std()  
    df['input_mean_value_usd'] = inputs['value_usd'].mean()
    df['input_std_value_usd'] = inputs['value_usd'].std()  
    df['input_mean_per_marketcap_usd'] = inputs['value_percent_marketcap'].mean()  
    df['input_std_per_marketcap_usd'] = inputs['value_percent_marketcap'].std() 
    
    df['output_mean_value_btc'] = outputs['value_btc'].mean()
    df['output_std_value_btc'] = outputs['value_btc'].std()  
    df['output_mean_value_usd'] = outputs['value_usd'].mean()
    df['output_std_value_usd'] = outputs['value_usd'].std()  
    df['output_mean_per_marketcap_usd'] = outputs['value_percent_marketcap'].mean()  
    df['output_std_per_marketcap_usd'] = outputs['value_percent_marketcap'].std() 
     
    #total transaction value (all addresses)
    df['input_mean_tx_value_btc'] = inputs['tx_value_btc'].mean()
    df['input_std_tx_value_btc'] = inputs['tx_value_btc'].std()  
    df['input_mean_tx_value_usd'] = inputs['tx_value_usd'].mean()
    df['input_std_tx_value_usd'] = inputs['tx_value_usd'].std()  
    
    df['outputs_mean_tx_value_btc'] = outputs['tx_value_btc'].mean()
    df['outputs_std_tx_value_btc'] = outputs['tx_value_btc'].std()  
    df['outputs_mean_tx_value_usd'] = outputs['tx_value_usd'].mean()
    df['outputs_std_tx_value_usd'] = outputs['tx_value_usd'].std()  
        
    #drop unneccessary columns
    final = df.drop(['block_number', 'block_timestamp', 'value_btc', 'hash',
           'input_count', 'output_count', 'tx_value_btc',
           'balance_btc', 'date', 'value_usd', 'tx_value_usd',
           'value_percent_marketcap', 'balance_usd', 'type', 'transaction_hash', 
           'CapMrktCurUSD', 'PriceUSD'], axis = 1) 
    final = final.iloc[[0]]
    return final


#########
df_features = pd.DataFrame()    
lock = threading.Lock()
len(df_features)
def start_engineer(list_address):
    df_features_local = pd.DataFrame()  
    
    for address in list_address:
        df = all_tnx_2 [all_tnx_2['address'] == address]
        final = feature_engineering(df)
        df_features_local = df_features_local.append(final)
        print(len(df_features_local), "/" , len(list_address), 'appended', sep=" ")       
        
    print(">>>THREAD FINISHED<<<")
    global df_features
    with lock:
        df_features = df_features.append(df_features_local)
  
   
# =============================================================================
#     
# =============================================================================
    
import dask.dataframe as dd    
    
#Load data
btc_price_data = dd.read_csv("data/btc_price_data.csv")
btc_price_data = btc_price_data[['date', 'CapMrktCurUSD','PriceUSD']]
btc_price_data['date'] = dd.to_datetime(btc_price_data['date']).apply(lambda x: '{year}-{month}-{day}'.format(year=x.year, month=x.month, day=x.day))  
 
#trainingsdata testsample 7 classes
#all_tnx = dd.read_csv("data/testdata_30k.csv")
#tmp = all_tnx.drop_duplicates(subset='address').compute()
#tmp = tmp.sample(n=10000, random_state = 1)['address']
#all_tnx = dd.merge(all_tnx,tmp,on='address',how='inner')

#Exhange dataset
#all_tnx = dd.read_csv("data/testdata_5k_exchange.csv")

#Unknown dataset
all_tnx = dd.read_csv("data/testdata_5k_unknown.csv")



#Add Dollar Price    
all_tnx['date'] = dd.to_datetime(all_tnx['block_timestamp']).apply(lambda x: '{year}-{month}-{day}'.format(year=x.year, month=x.month, day=x.day))
all_tnx = dd.merge(all_tnx, btc_price_data, on='date', how='inner')
all_tnx['value_usd'] = all_tnx['value_btc'] * all_tnx['PriceUSD']
all_tnx['tx_value_usd'] = all_tnx['tx_value_btc'] * all_tnx['PriceUSD']
all_tnx['value_percent_marketcap'] = (all_tnx['value_usd'] / all_tnx['CapMrktCurUSD']) *100
all_tnx['block_timestamp'] = dd.to_datetime(all_tnx['block_timestamp'])
all_tnx['balance_btc'] = 0.0 

#Multithrading
addresses = all_tnx.drop_duplicates(subset='address').compute()
addresses = addresses['address'].to_list()
addresses_list = np.array_split(addresses, 200)

#Convert dask df to pandas df
all_tnx_2 = all_tnx.compute()

for counter, list_address in enumerate(addresses_list):   
    thread = threading.Thread(target=start_engineer, args=(list_address,))
    thread.start() 
    print('Thread started', counter, sep=" ")
   
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
    


