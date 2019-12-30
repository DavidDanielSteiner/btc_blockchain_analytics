# -*- coding: utf-8 -*-
"""
Created on Mon Nov 11 18:33:32 2019

@author: David
"""
import pandas as pd
import dask.dataframe as dd    

def merge_data(btc_price_data, tnx, wallets):      
    wallets = wallets.dropna()
    wallets = wallets.drop_duplicates(subset='address')
    

    #Preprocess transactions
    tnx['date'] = dd.to_datetime(tnx['block_timestamp']).apply(lambda x: '{year}-{month}-{day}'.format(year=x.year, month=x.month, day=x.day))
    
    #Merge trnsactions with wallet labels
    sender = dd.merge(tnx, wallets, left_on='sender', right_on='address', how='left')
    sender = sender.rename(columns = {"owner": "sender_name", "category":"sender_category"}) 
    sender = sender.drop(['address'], axis=1)    
    receiver = dd.merge(tnx, wallets, left_on='receiver', right_on='address', how='left')
    receiver = receiver.rename(columns = {"owner": "receiver_name", "category":"receiver_category"}) 
    receiver = receiver.drop(['address'], axis=1)
    
    tnx_labeled = dd.merge(sender, receiver,  how='inner', on=['hash', 'block_timestamp', 'sender','receiver', 'date', 'value'])
    tnx_labeled = tnx_labeled.rename(columns = {"value": "btc"}) 
    
    #Merge with price data
    btc_price_data = btc_price_data[['date', 'CapMrktCurUSD','PriceUSD']]
    btc_price_data['date'] = dd.to_datetime(btc_price_data['date']).apply(lambda x: '{year}-{month}-{day}'.format(year=x.year, month=x.month, day=x.day))   

    transactions = dd.merge(tnx_labeled, btc_price_data, on='date', how='inner')
    transactions['dollar'] = transactions['btc'] * transactions['PriceUSD']
    transactions['percent_marketcap'] = (transactions['dollar'] / transactions['CapMrktCurUSD']) *100
    
    #Dask dataframe to pandas dataframe
    transactions = transactions.compute()    
    return transactions


def filter_data(data, filter_type, value):
    if filter_type == 'dollar':
        filtered_transactions = data[data['dollar'] >= value]
    elif filter_type == 'marketcap':
        filtered_transactions = data[data['percent_marketcap'] >= value]
    
    filter_name = str(value) + "_" + filter_type 
    return filter_name, filtered_transactions


def get_unknown_wallets(df):
    #get addresses that have no label
    sender = df[['sender', 'sender_name']]
    sender = sender[sender['sender_name'].isna()]
    sender.rename(columns = {"sender" : 'address'}, inplace = True) 
    
    receiver = df[['receiver', 'receiver_name']]
    receiver = receiver[receiver['receiver_name'].isna()]
    receiver.rename(columns = {"receiver" : 'address'}, inplace = True) 
    
    missing_labels = sender.append(receiver)
    missing_labels = missing_labels[['address']]
    missing_labels = missing_labels.drop_duplicates(keep='last')    
    return missing_labels


