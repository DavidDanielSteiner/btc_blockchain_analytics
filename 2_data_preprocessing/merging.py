# -*- coding: utf-8 -*-
"""
Created on Mon Nov 11 18:33:32 2019

@author: David
"""
import pandas as pd

def merge_data():   
    #import
    btc_price_data = pd.read_csv("data/btc_price_data.csv") #https://coinmetrics.io/community-data-dictionary/   #https://coinmetrics.io/newdata/btc.csv
    tnx = pd.read_csv("data/transactions_500BTC.csv")
    #tmp = tnx[tnx['value'] >= 100]
    #tnx = tmp
    wallets_1 = pd.read_csv("data/wallets.csv", index_col=False)
    #wallets_1 = pd.read_csv("data/wallets_walletexplorer.csv", index_col=False)
    #wallets_2 = pd.read_csv("data/wallets_bitinfocharts.csv", index_col=False)
    #wallets_3 = pd.read_csv("data/wallets_bitinfocharts_missing.csv", index_col=False)
    #wallets_4 = pd.read_csv("data/wallets_cryptoground.csv", index_col=False)
        
    #Merge with wallets
    wallets = wallets_1[['address', 'owner', 'category']]
    #wallets = wallets.append(wallets_2.dropna())
    #wallets = wallets.append(wallets_3.dropna())
    #wallets = wallets.append(wallets_4.dropna())
    wallets = wallets.drop_duplicates(subset='address', keep='last')
    
    #change specific owner and category values
    wallets.loc[wallets.owner == 'Xapo.com-2', 'owner'] = 'Xapo.com'
    wallets.loc[wallets.owner == 'Bitfinex.com', 'owner'] = 'Bitfinex'
    wallets.loc[wallets.owner == 'DPR Seized Coins 2', 'category'] = 'Service'
    wallets.loc[wallets.owner == 'F2Pool', 'category'] = 'Pools'

    #Preprocess transactions
    tnx['date'] = pd.to_datetime(tnx['block_timestamp']).apply(lambda x: '{year}-{month}-{day}'.format(year=x.year, month=x.month, day=x.day))
    
    #Merge trnsactions with wallet labels
    sender = pd.merge(tnx, wallets, left_on='sender', right_on='address', how='left')
    sender.rename(columns = {"owner": "sender_name", "category":"sender_category"}, inplace = True) 
    columns = ['address']
    sender = sender.drop(columns, axis=1)    
    receiver = pd.merge(tnx, wallets, left_on='receiver', right_on='address', how='left')
    receiver.rename(columns = {"owner": "receiver_name", "category":"receiver_category"}, inplace = True) 
    columns = ['address']
    receiver = receiver.drop(columns, axis=1)
    
    tnx_labeled = pd.merge(sender, receiver,  how='inner', on=['hash', 'block_timestamp', 'sender','receiver', 'date', 'value'])
    tnx_labeled.rename(columns = {"value": "btc"}, inplace = True) 
    
    #Merge with price data
    btc_price_data = btc_price_data[['date', 'CapMrktCurUSD','PriceUSD']]
    btc_price_data['date'] = pd.to_datetime(btc_price_data['date']).apply(lambda x: '{year}-{month}-{day}'.format(year=x.year, month=x.month, day=x.day))   
    #btc_price_data['date'] = pd.to_datetime(btc_price_data['date'])
            
    data = pd.merge(tnx_labeled, btc_price_data, on='date', how='inner')
    data['dollar'] = data['btc'] * data['PriceUSD']
    data['percent_marketcap'] = (data['dollar'] / data['CapMrktCurUSD']) *100
    
    return data, wallets


def filter_data(data, filter_type, value):
    if filter_type == 'dollar':
        filtered_transactions = data[data['dollar'] >= value]
    elif filter_type == 'marketcap':
        filtered_transactions = data[data['percent_marketcap'] >= value]
    return filtered_transactions


def get_unknown_wallets(df):
    #get distinct addresses that have no label
    sender = df[['sender', 'sender_name']]
    sender = sender[sender['sender_name'].isna()]
    sender.rename(columns = {"sender" : 'address'}, inplace = True) 
    
    receiver = df[['receiver', 'receiver_name']]
    receiver = receiver[receiver['receiver_name'].isna()]
    receiver.rename(columns = {"receiver" : 'address'}, inplace = True) 
    
    missing_labels = sender.append(receiver)
    missing_labels = missing_labels[['address']]
    missing_labels = missing_labels.drop_duplicates(keep='last')
    
    missing_labels.to_csv("unkown_wallets.csv", index = False)
    print("saved unknown wallets as csv")


# =============================================================================
# Start
# =============================================================================

data, wallets = merge_data()

filtered_tnx = filter_data(data, filter_type = 'dollar', value=500000)
filtered_tnx.to_csv("transactions_filtered_1MIO.csv", index=False)
wallets.to_csv("wallets.csv", index=False)

get_unknown_wallets(filtered_tnx)


