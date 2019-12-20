# -*- coding: utf-8 -*-
"""
Created on Mon Dec 16 14:49:53 2019

@author: David
"""

import pandas as pd
import threading
import numpy as np

btc_price_data = pd.read_csv("data/btc_price_data.csv")
btc_price_data = btc_price_data[['date', 'CapMrktCurUSD','PriceUSD']]
btc_price_data['date'] = pd.to_datetime(btc_price_data['date']).apply(lambda x: '{year}-{month}-{day}'.format(year=x.year, month=x.month, day=x.day))  
 
def feature_engineering(df):
    df = df.sort_values('block_number')    
    df = df.reset_index(drop=True)
    
    df['block_timestamp'] = pd.to_datetime(df['block_timestamp'])
    df["value_btc"] = pd.to_numeric(df["value_btc"])
    df["tx_value_btc"] = pd.to_numeric(df["tx_value_btc"])
    df['balance_btc'] = 0.0
    
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
     
    #Add Dollar Price    
    df['date'] = pd.to_datetime(df['block_timestamp']).apply(lambda x: '{year}-{month}-{day}'.format(year=x.year, month=x.month, day=x.day))
    df = pd.merge(df, btc_price_data, on='date', how='inner')
    
    df['value_usd'] = df['value_btc'] * df['PriceUSD']
    df['tx_value_usd'] = df['tx_value_btc'] * df['PriceUSD']
    df['value_percent_marketcap'] = (df['value_usd'] / df['CapMrktCurUSD']) *100
    df['balance_usd'] = df['balance_btc'] * df['PriceUSD']
    
    df = df.drop(['transaction_hash', 'CapMrktCurUSD', 'PriceUSD'], axis = 1) 
    
    
    #feature engineering
    tx = df.sort_values('type') 
    tx = tx.drop_duplicates(subset='hash', keep='first') #keep inputs
    tx_type = df.drop_dublicates(subset=['hash', 'type'], keep='first')
    
    df['n_tx'] = len(tx)
    df['lifetime'] = ((max(df['block_timestamp']) - min(df['block_timestamp'])).days) +1
    df['tx_per_day'] = df['n_tx'] / df['lifetime']
    
    inputs = tx[tx['type'] == 'input']
    outputs = tx[tx['type'] == 'output']
    df['n_inputs'] = len(inputs)
    df['n_outputs'] = len(outputs)
    df['p_received'] = len(inputs) / df['n_tx']
    
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
           'value_percent_marketcap', 'balance_usd', 'type'], axis = 1) 
    
    final = final.iloc[[0]]
    return final


#########
df_features = pd.DataFrame()    

def start_engineer(list_address):
    for address in list_address:
        df = all_tnx[all_tnx['address'] == address]
        final = feature_engineering(df)
        global df_features
        df_features = df_features.append(final)
        print(address, len(df_features), "/" , len(addresses), 'appended', sep=" ")
    
#Load data
all_tnx = pd.read_csv("data/testdata_30k.csv", index_col=False)
addresses = all_tnx.drop_duplicates(subset='address')['address'].to_list()
print('Dataset loaded')

#Multithrading
addresses_list = np.array_split(addresses, 50)

for counter, list_address in enumerate(addresses_list):   
    thread_engineer = threading.Thread(target=start_engineer, args=(list_address,))
    thread_engineer.start() 
    print('Thread started')
    
     
########
    
#Data for sql
offchain = pd.read_csv("data/offchain.csv", index_col=False)
offchain = offchain.dropna(subset=['class'])
#list_addresses = offchain.sample(n=100, random_state = 1)['address'].to_list()
list_addresses = offchain['address'].to_list()

#Export Data
category = offchain[['address', 'class']]
df_features_2 = pd.merge(df_features,category,on='address',how='inner')
#df_features = df_features.drop(['address'], axis = 1)     
df_features_2.to_csv("testdata_30k_features.csv", index=False)    
    


