# -*- coding: utf-8 -*-
"""
Created on Mon Nov 25 16:44:34 2019

@author: David
"""

import pandas as pd
import random
import string
#import dask.dataframe as dd    


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
    sender = sender.rename(columns = {"sender_name" : 'owner'}) 
    sender = aggregate_most_common(sender)
    sender = sender.rename(columns = {"owner" : 'sender_name2'}) 
    
    receiver = df[['hash', 'receiver_name']]
    receiver = receiver.rename(columns = {"receiver_name" : 'owner'}) 
    receiver = aggregate_most_common(receiver)
    receiver = receiver.rename(columns = {"owner" : 'receiver_name2'}) 
    
    if unique:
        df = df.drop_duplicates(subset='hash', keep='last')
        
    df_grouped = df.merge(sender, on="hash", how="inner")
    df_grouped = df_grouped.merge(receiver, on="hash", how="inner")
    return df_grouped


def regroup(df):
    sender = df[['sender', 'sender_name2']]
    sender = sender[sender['sender_name2'] != 'unknown']
    receiver = df[['receiver', 'receiver_name2']]
    receiver = receiver[receiver['receiver_name2'] != 'unknown']
    
    receiver = receiver.rename(columns = {"receiver_name2" : 'owner',
                          "receiver" : 'address'}, inplace = True) 
    
    sender.rename(columns = {"sender_name2" : 'owner',
                          "sender" : 'address'}, inplace = True) 
    
    labeled_wallets = receiver.append(sender)
    labeled_wallets = labeled_wallets.drop_duplicates(keep='last') 
    labeled_wallets['category'] = 'TMP'
    return labeled_wallets


def add_category(wallet_owners, df):
    sender = pd.merge(df, wallet_owners, left_on='sender_name', right_on='owner', how='left')    
    columns = ['receiver_name', 'receiver_category', 'sender_name', 'sender_category']
    sender = sender.drop(columns, axis=1)    
    sender = sender.rename(columns = {"owner": "sender_name", "category":"sender_category"}) 

    receiver = receiver = pd.merge(df, wallet_owners, left_on='receiver_name', right_on='owner', how='left')
    columns = ['sender_name', 'sender_category', 'receiver_name', 'receiver_category']
    receiver = receiver.drop(columns, axis=1)
    receiver = receiver.rename(columns = {"owner": "receiver_name", "category":"receiver_category"}) 
    
    tnx_category = pd.merge(sender, receiver,  how='inner', on=['hash', 'block_timestamp', 'sender','receiver', 'date', 'btc', 'dollar', 'percent_marketcap', 'PriceUSD'])
    tnx_category = tnx_category[['hash', 'block_timestamp', 'sender', 'receiver', 'btc', 'dollar', 'PriceUSD', 'percent_marketcap', 'sender_name', 'sender_category', 'receiver_name', 'receiver_category']]   
    return tnx_category

    
def merge_tnx_wallets(tnx, wallets, new_wallets):
    #Merge trnsactions with wallet labels
    wallets = wallets.append(new_wallets)
    wallets = wallets.drop_duplicates(subset='address', keep='last')
     
    sender = pd.merge(tnx, wallets, left_on='sender', right_on='address', how='left')
    sender = sender.rename(columns = {"owner": "sender_name", "category":"sender_category"}) 
    sender = sender.drop(['address'], axis=1)    
    
    receiver = pd.merge(tnx, wallets, left_on='receiver', right_on='address', how='left')
    receiver = receiver.rename(columns = {"owner": "receiver_name", "category":"receiver_category"}) 
    receiver = receiver.drop(['address'], axis=1)
    
    tnx_labeled = pd.merge(sender, receiver,  how='inner', on=['hash', 'block_timestamp', 'sender','receiver', 'date', 'btc', 'dollar', 'percent_marketcap', 'PriceUSD'])
    return tnx_labeled
