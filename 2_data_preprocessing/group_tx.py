# -*- coding: utf-8 -*-
"""
Created on Mon Nov 25 16:44:34 2019

@author: David
"""

import pandas as pd
import random
import string


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
    sen = df[['sender', 'sender_name2']]
    sen = sen[sen['sender_name2'] != 'unknown']
    rec = df[['receiver', 'receiver_name2']]
    rec = rec[rec['receiver_name2'] != 'unknown']
    
    rec.rename(columns = {"receiver_name2" : 'owner',
                          "receiver" : 'address'}, inplace = True) 
    
    sen.rename(columns = {"sender_name2" : 'owner',
                          "sender" : 'address'}, inplace = True) 
    
    allx = rec.append(sen)
    allx = allx.drop_duplicates(keep='last') 
    allx['category'] = 'Exchange'
    return allx


def add_category(wallet_owners, df):
    sender = pd.merge(df, wallet_owners, left_on='sender_name', right_on='owner', how='left')    
    columns = ['receiver_name', 'receiver_category', 'sender_name', 'sender_category']
    sender = sender.drop(columns, axis=1)    
    sender.rename(columns = {"owner": "sender_name", "category":"sender_category"}, inplace = True) 

    receiver = pd.merge(df, wallet_owners, left_on='receiver_name', right_on='owner', how='left')
    columns = ['sender_name', 'sender_category', 'receiver_name', 'receiver_category']
    receiver = receiver.drop(columns, axis=1)
    receiver.rename(columns = {"owner": "receiver_name", "category":"receiver_category"}, inplace = True) 
    
    tnx_category = pd.merge(sender, receiver,  how='inner', on=['hash', 'block_timestamp', 'sender','receiver', 'date', 'btc', 'dollar', 'percent_marketcap', 'PriceUSD'])
    tnx_category = tnx_category[['hash', 'block_timestamp', 'sender', 'receiver', 'btc', 'dollar', 'PriceUSD', 'percent_marketcap', 'sender_name', 'sender_category', 'receiver_name', 'receiver_category']]
    
    return tnx_category

    
def merge_tnx_wallets(tnx, wallets, new_wallets):
    #Merge trnsactions with wallet labels
    wallets = wallets.append(new_wallets)
    wallets = wallets.drop_duplicates(subset='address', keep='last')
     
    sender = pd.merge(tnx, wallets, left_on='sender', right_on='address', how='left')
    sender.rename(columns = {"owner": "sender_name", "category":"sender_category"}, inplace = True) 
    columns = ['address']
    sender = sender.drop(columns, axis=1)    
    
    receiver = pd.merge(tnx, wallets, left_on='receiver', right_on='address', how='left')
    receiver.rename(columns = {"owner": "receiver_name", "category":"receiver_category"}, inplace = True) 
    columns = ['address']
    receiver = receiver.drop(columns, axis=1)
    
    tnx_labeled = pd.merge(sender, receiver,  how='inner', on=['hash', 'block_timestamp', 'sender','receiver', 'date', 'btc', 'dollar', 'percent_marketcap', 'PriceUSD'])
 
    return tnx_labeled




'''
wallet_owners = wallets[['owner', 'category']].drop_duplicates(subset='owner', keep='last').reset_index(drop=True)
labeled_tnx = add_category(wallet_owners, df)
filtered_transactions = labeled_tnx[labeled_tnx['dollar'] >= 10000000]
sen = filtered_transactions[ (filtered_transactions['receiver_category'] == 'Exchange') | (filtered_transactions['sender_category'] == 'Exchange') ]
sen = filtered_transactions[filtered_transactions['receiver_category'] == 'Services']
sen = filtered_transactions[filtered_transactions['receiver_name'] == 'Kraken.com']
#df_grouped.to_csv("transactions_10MIO.csv", index=False)
#df_unique = group_transactions(filtered_transactions, unique=True)
#df_unique.rename(columns = {"block_timestamp" : 'date', 'dollar':'usd'}, inplace = True) 
#df_unique = df_unique[['hash', 'date', 'btc', 'usd', 'sender_name', 'sender_category', 'receiver_name', 'receiver_category' ]]
#df_unique.to_csv("transactions_unique_10MIO.csv", index=False)
tmp = pd.merge(sen, wallets, left_on='sender', right_on='address', how="left")
x = tmp[['hash', 'sender_name', 'owner', 'receiver_name']]
'''



