# -*- coding: utf-8 -*-
"""
Created on Mon Dec 16 14:49:53 2019

@author: David
"""

import pandas as pd
import threading
import numpy as np
import dask.dataframe as dd    


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
    
    df['output_mean_value_btc'] = outputs['value_btc'].mean()
    df['output_std_value_btc'] = outputs['value_btc'].std()  
    df['output_mean_value_usd'] = outputs['value_usd'].mean()
    df['output_std_value_usd'] = outputs['value_usd'].std()  
    
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


df_features = pd.DataFrame()    
lock = threading.Lock()

def handle_threads(list_address, all_tnx):
    df_features_local = pd.DataFrame()  
    
    for address in list_address:
        df = all_tnx [all_tnx['address'] == address]
        final = feature_engineering(df)
        df_features_local = df_features_local.append(final)
        print(len(df_features_local), "/" , len(list_address), 'appended', sep=" ")       
        
    print(">>>THREAD FINISHED<<<")
    global df_features
    with lock:
        df_features = df_features.append(df_features_local)
  
   
def get_features(all_tnx):
    #Add Dollar Price    
    btc_price_data = dd.read_csv("../data/btc_price_data.csv")
    btc_price_data = btc_price_data[['date', 'CapMrktCurUSD','PriceUSD']]
    btc_price_data['date'] = dd.to_datetime(btc_price_data['date']).apply(lambda x: '{year}-{month}-{day}'.format(year=x.year, month=x.month, day=x.day))  
     
    all_tnx['date'] = dd.to_datetime(all_tnx['block_timestamp']).apply(lambda x: '{year}-{month}-{day}'.format(year=x.year, month=x.month, day=x.day))
    all_tnx = dd.merge(all_tnx, btc_price_data, on='date', how='inner')
    all_tnx['value_usd'] = all_tnx['value_btc'] * all_tnx['PriceUSD']
    all_tnx['tx_value_usd'] = all_tnx['tx_value_btc'] * all_tnx['PriceUSD']
    all_tnx['value_percent_marketcap'] = (all_tnx['value_usd'] / all_tnx['CapMrktCurUSD']) *100
    all_tnx['block_timestamp'] = dd.to_datetime(all_tnx['block_timestamp'])
    all_tnx['balance_btc'] = 0.0 
    
    #Multithrading
    print("Split data into 200 threads")
    addresses = all_tnx.drop_duplicates(subset='address').compute()
    addresses = addresses['address'].to_list()
    addresses_list = np.array_split(addresses, 200)
    
    #Convert dask df to pandas df
    all_tnx = all_tnx.compute()
    
    for counter, list_address in enumerate(addresses_list):   
        thread = threading.Thread(target=handle_threads, args=(list_address, all_tnx))
        thread.start() 
        print('Thread started', counter, sep=" ")
        
    thread.join()
    global df_features
    print('All Threads finished')
    print(len(df_features))
    
    return df_features

   