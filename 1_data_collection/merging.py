# -*- coding: utf-8 -*-
"""
Created on Mon Nov 11 18:33:32 2019

@author: David
"""
import pandas as pd
import random
import string

#import
btc_price_data = pd.read_csv("data/btc_price_data.csv") #https://coinmetrics.io/community-data-dictionary/
btc_price_data = btc_price_data[['date', 'CapMrktCurUSD','PriceUSD']]
tnx = pd.read_csv("data/transactions_over_500BTC.csv")
wallets_1 = pd.read_csv("data/wallets_walletexplorer.csv", index_col=False)
wallets_2 = pd.read_csv("data/wallets_bitinfocharts.csv", index_col=False)
wallets_3 = pd.read_csv("data/wallets_bitinfocharts_missing.csv", index_col=False)
wallets_4 = pd.read_csv("data/wallets_cryptoground.csv", index_col=False)


#Preprocess transactions
tnx['date'] = pd.to_datetime(tnx['block_timestamp']).apply(lambda x: '{year}-{month}-{day}'.format(year=x.year, month=x.month, day=x.day))

def merge_filter(new_wallet):
    #Merge with wallets
    wallets = wallets_1[['address', 'owner', 'category']]
    wallets = wallets.append(wallets_2.dropna())
    wallets = wallets.append(wallets_3.dropna())
    wallets = wallets.append(wallets_4.dropna())
    wallets = wallets.append(new_wallet)
    wallets = wallets.drop_duplicates(subset='address', keep='last')

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
    btc_price_data['date'] = pd.to_datetime(btc_price_data['date']).apply(lambda x: '{year}-{month}-{day}'.format(year=x.year, month=x.month, day=x.day))   
    data = pd.merge(tnx_labeled, btc_price_data, on='date', how='inner')
    data['dollar'] = data['btc'] * data['PriceUSD']
    data['percent_marketcap'] = (data['dollar'] / data['CapMrktCurUSD']) *100
    
    #filter
    filtered_transactions = data[data['dollar'] >= 10000000]
    #filtered_transactions = data[data['percent_marketcap'] >= 0.5]

    #filtered_transactions.to_csv("transactions_10MIO.csv", index=False)
    return filtered_transactions

def randomString(x):
    """Generate a random string of fixed length """
    stringLength=30
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))

def check_owner(x):    
    if not isinstance(x, str):
        x = sorted(x, key=len)
        x = x[0]
    
    length = len(x)
    if length == 30:
        return 'unknown'
    else:
        return x

def aggregate_most_common(df):
    df = df[['hash', 'owner']]
    tmp = df[df['owner'].isna()]
    tmp['owner'] = tmp['owner'].apply(randomString)
    df = df.dropna()
    df = df.append(tmp)
    df = df.groupby(['hash'], as_index=False)['owner'].agg(pd.Series.mode) #https://stackoverflow.com/questions/15222754/groupby-pandas-dataframe-and-select-most-common-value
    df['owner'] = df['owner'].apply(check_owner)
    return df

def group_transactions(df, unique=False):
    sender = df[['hash', 'sender_name']]
    sender.rename(columns = {"sender_name" : 'owner'}, inplace = True) 
    sender = aggregate_most_common(sender)
    sender.rename(columns = {"owner" : 'sender_name2'}, inplace = True) 
    
    receiver = df[['hash', 'receiver_name']]
    receiver.rename(columns = {"receiver_name" : 'owner'}, inplace = True) 
    receiver = aggregate_most_common(receiver)
    receiver.rename(columns = {"owner" : 'receiver_name2'}, inplace = True) 
    
    if unique:
        df = df.drop_duplicates(subset='hash', keep='last')
        
    df_grouped = df.merge(sender, on="hash", how="inner")
    df_grouped = df_grouped.merge(receiver, on="hash", how="inner")
    return df_grouped


def regroup(df):
    tmp = df
    sen = tmp[['sender', 'sender_name2']]
    sen = sen[sen['sender_name2'] != 'unknown']
    rec = tmp[['receiver', 'receiver_name2']]
    rec = rec[rec['receiver_name2'] != 'unknown']
    
    rec.rename(columns = {"receiver_name2" : 'owner',
                          "receiver" : 'address'}, inplace = True) 
    
    sen.rename(columns = {"sender_name2" : 'owner',
                          "sender" : 'address'}, inplace = True) 
    
    allx = rec.append(sen)
    allx = allx.drop_duplicates(keep='last') 
    allx['category'] = 'Exchange'
    return allx
    #allx.to_csv('wallets_4.csv', mode='a', index=False)


def get_missing_labels(df):
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
    
    missing_labels.to_csv("missing_labels.csv", index = False)
    
    
new_wallets = pd.DataFrame()

for i in range(8):
    filtered_transactions = merge_filter(new_wallets)
    df_grouped = group_transactions(filtered_transactions)
    new_wallets = new_wallets.append(regroup(df_grouped)).drop_duplicates(keep='last')
    print(len(new_wallets))



df_grouped.to_csv("transactions_10MIO.csv", index=False)

df_unique = group_transactions(filtered_transactions, unique=True)
df_unique.rename(columns = {"block_timestamp" : 'date', 'dollar':'usd'}, inplace = True) 
df_unique = df_unique[['hash', 'date', 'btc', 'usd', 'sender_name', 'sender_category', 'receiver_name', 'receiver_category' ]]
df_unique.to_csv("transactions_unique_10MIO.csv", index=False)


