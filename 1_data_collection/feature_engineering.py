# -*- coding: utf-8 -*-
"""
Created on Mon Dec 16 14:49:53 2019

@author: David
"""

import pandas as pd


def feature_engineering(df):
    df = df.sort_values('block_number')    
    df = df.reset_index(drop=True)
    
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
    btc_price_data = pd.read_csv("data/btc_price_data.csv")
    btc_price_data = btc_price_data[['date', 'CapMrktCurUSD','PriceUSD']]
    btc_price_data['date'] = pd.to_datetime(btc_price_data['date']).apply(lambda x: '{year}-{month}-{day}'.format(year=x.year, month=x.month, day=x.day))  
    
    df['date'] = pd.to_datetime(df['block_timestamp']).apply(lambda x: '{year}-{month}-{day}'.format(year=x.year, month=x.month, day=x.day))
    df = pd.merge(df, btc_price_data, on='date', how='inner')
    
    df['value_usd'] = df['value_btc'] * df['PriceUSD']
    df['tx_value_usd'] = df['tx_value_btc'] * df['PriceUSD']
    df['value_percent_marketcap'] = (df['value_usd'] / df['CapMrktCurUSD']) *100
    df['balance_usd'] = df['balance_btc'] * df['PriceUSD']
    
    df = df.drop(['transaction_hash', 'CapMrktCurUSD', 'PriceUSD'], axis = 1) 
    
    
    #feature engineering
    tx = df.drop_duplicates(subset='hash')
    
    df['n_tx'] = len(tx)
    df['lifetime'] = (max(df['block_timestamp']) - min(df['block_timestamp'])).days
    df['tx_per_day'] = df['n_tx'] / df['lifetime']
    
    inputs = tx[tx['type'] == 'input']
    outputs = tx[tx['type'] == 'output']
    df['n_inputs'] = len(inputs)
    df['n_outputs'] = len(outputs)
    df['p_received'] = len(inputs) / len(tx)
    
    usd_per_tx = df[['hash', 'value_usd']].groupby('hash').agg({'value_usd':'sum'})
    df['p_0k'] = len(usd_per_tx[usd_per_tx['value_usd'] <= 100]) / len(tx)
    df['p_1k'] = len(usd_per_tx[ (usd_per_tx['value_usd'] >= 100) & (usd_per_tx['value_usd'] < 1000)]) / len(tx)
    df['p_10k'] = len(usd_per_tx[ (usd_per_tx['value_usd'] >= 1000) & (usd_per_tx['value_usd'] < 10000)]) / len(tx)
    df['p_100k'] = len(usd_per_tx[ (usd_per_tx['value_usd'] >= 10000) & (usd_per_tx['value_usd'] < 100000)]) / len(tx)
    df['p_1m'] = len(usd_per_tx[ (usd_per_tx['value_usd'] >= 100000) & (usd_per_tx['value_usd'] < 1000000)]) / len(tx)
    df['p_10m'] = len(usd_per_tx[ (usd_per_tx['value_usd'] >= 1000000) & (usd_per_tx['value_usd'] < 10000000)]) / len(tx)
    df['p_100m'] = len(usd_per_tx[ (usd_per_tx['value_usd'] >= 10000000) & (usd_per_tx['value_usd'] < 100000000)]) / len(tx)
    df['p_1b'] = len(usd_per_tx[ (usd_per_tx['value_usd'] >= 100000000) & (usd_per_tx['value_usd'] < 1000000000)]) / len(tx)
    df['p_10b'] = len(usd_per_tx[usd_per_tx['value_usd'] >= 1000000000]) / len(tx)
    
    all_in = df[df['type'] == 'input']
    all_out = df[df['type'] == 'output']
    payback = pd.merge(all_in,all_out, how='inner', on='hash')
    df['n_payback'] = len(payback)
    df['p_payback'] = len(payback) / len(tx)
    
    df['mean_inputs'] = inputs['input_count'].mean()
    df['std_inputs'] = inputs['input_count'].std()
    df['mean_outputs'] = outputs['output_count'].mean()
    df['std_outputs'] = outputs['output_count'].std()
    
    df['mean_balance_btc'] = df['balance_btc'].mean()   
    df['std_balance_btc']  = df['balance_btc'].std() 
    df['mean_balance_usd'] = df['balance_usd'].mean()  
    df['std_balance_usd'] = df['balance_usd'].std() 
    
    df['mean_per_marketcap_usd'] = df['value_percent_marketcap'].mean()  
    df['std_per_marketcap_usd'] = df['value_percent_marketcap'].std() 
    
    df['adr_inputs_btc'] = df[df['type'] == 'input']['value_btc'].sum()
    df['adr_outputs_btc'] = df[df['type'] == 'output']['value_btc'].sum()
    df['adr_inputs_usd'] = df[df['type'] == 'input']['value_usd'].sum()
    df['adr_outputs_usd'] = df[df['type'] == 'output']['value_usd'].sum()
    df['adr_dif_usd'] = df['adr_outputs_usd'] - df['adr_inputs_usd']
    
    df['input_mean_tx_value_btc'] = inputs['tx_value_btc'].mean()
    df['input_std_tx_value_btc'] = inputs['tx_value_btc'].std()  
    df['input_mean_tx_value_usd'] = inputs['tx_value_usd'].mean()
    df['input_std_tx_value_usd'] = inputs['tx_value_usd'].std()  
    
    df['outputs_mean_tx_value_btc'] = outputs['tx_value_btc'].mean()
    df['outputs_std_tx_value_btc'] = outputs['tx_value_btc'].std()  
    df['outputs_mean_tx_value_usd'] = outputs['tx_value_usd'].mean()
    df['outputs_std_tx_value_usd'] = outputs['tx_value_usd'].std()  
    
    
    final = df.drop(['block_number', 'block_timestamp', 'value_btc', 'hash',
           'input_count', 'output_count', 'tx_value_btc',
           'balance_btc', 'date', 'value_usd', 'tx_value_usd',
           'value_percent_marketcap', 'balance_usd', 'type'], axis = 1) 
    final = final.iloc[[0]]
    return final


#########
    
offchain = pd.read_csv("data/offchain.csv", index_col=False)
offchain = offchain.dropna(subset=['class'])
#list_addresses = offchain.sample(n=100, random_state = 1)['address'].to_list()
list_addresses = offchain['address'].to_list()


all_tnx = pd.read_csv("data/testdata_30k.csv", index_col=False)
addresses = all_tnx.drop_duplicates(subset='address')['address'].to_list()

df_features = pd.DataFrame()

for address in addresses:
    df = all_tnx[all_tnx['address'] == address]
    final = feature_engineering(df)
    df_features = df_features.append(final)
    print(address, 'appended', sep=" ")
  
category = offchain[['address', 'class']]
df_features = pd.merge(df_features,category,on='address',how='inner')
df_features = df_features.drop(['address'], axis = 1)     
    
    