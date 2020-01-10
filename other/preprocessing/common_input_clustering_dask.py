# -*- coding: utf-8 -*-
"""
Created on Mon Nov 25 16:44:34 2019

@author: David
"""

import pandas as pd
import random
import string
import dask.dataframe as dd    




def chunk(s):
    return s.value_counts()


def agg(s):
    # s is a grouped multi-index series. In .apply the full sub-df will passed
    # multi-index and all. Group on the value level and sum the counts. The
    # result of the lambda function is a series. Therefore, the result of the 
    # apply is a multi-index series like (group, value): count
    #return s.apply(lambda s: s.groupby(level=-1).sum())

    # faster version using pandas internals
    s = s._selected_obj
    return s.groupby(level=list(range(s.index.nlevels))).sum()


def finalize(s):
    level = list(range(s.index.nlevels - 1))
    return (
        s.groupby(level=level)
        .apply(lambda s: s.reset_index(level=level, drop=True).argmax())
    )

mode = dd.Aggregation('mode', chunk, agg, finalize)



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
    #df = df.compute()
    #df = df.groupby(['hash'], as_index = False)['owner'].agg(pd.Series.mode) #https://stackoverflow.com/questions/15222754/groupby-pandas-dataframe-and-select-most-common-value
    #df = dd.from_pandas(df, npartitions=6)
    df = df.groupby(['hash'])['owner'].agg({'owner': mode}).reset_index()
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
                          "receiver" : 'address'}) 
    
    sender.rename(columns = {"sender_name2" : 'owner',
                          "sender" : 'address'}) 
    
    labeled_wallets = receiver.append(sender)
    labeled_wallets = labeled_wallets.drop_duplicates(keep='last') 
    labeled_wallets['category'] = 'TMP'
    return labeled_wallets


def add_category(wallets, df):   
    wallet_owners = wallets[['owner', 'category']].drop_duplicates(subset='owner', keep='last').reset_index(drop=True)
    
    sender = dd.merge(df, wallet_owners, left_on='sender_name', right_on='owner', how='left')    
    columns = ['receiver_name', 'receiver_category', 'sender_name', 'sender_category']
    sender = sender.drop(columns, axis=1)    
    sender = sender.rename(columns = {"owner": "sender_name", "category":"sender_category"}) 

    receiver = receiver = dd.merge(df, wallet_owners, left_on='receiver_name', right_on='owner', how='left')
    columns = ['sender_name', 'sender_category', 'receiver_name', 'receiver_category']
    receiver = receiver.drop(columns, axis=1)
    receiver = receiver.rename(columns = {"owner": "receiver_name", "category":"receiver_category"}) 
    
    tnx_category = dd.merge(sender, receiver,  how='inner', on=['hash', 'block_timestamp', 'sender','receiver', 'date', 'btc', 'dollar', 'percent_marketcap', 'PriceUSD'])
    tnx_category = tnx_category[['hash', 'block_timestamp', 'sender', 'receiver', 'btc', 'dollar', 'PriceUSD', 'percent_marketcap', 'sender_name', 'sender_category', 'receiver_name', 'receiver_category']]   
    return tnx_category

    
def merge_tnx_wallets(tnx, wallets, new_wallets):
    #Merge trnsactions with wallet labels
    wallets = wallets.append(new_wallets).drop_duplicates(subset='address', keep='last')
     
    sender = dd.merge(tnx, wallets, left_on='sender', right_on='address', how='left')
    sender = sender.rename(columns = {"owner": "sender_name", "category":"sender_category"}) 
    sender = sender.drop(['address'], axis=1)    
    
    receiver = dd.merge(tnx, wallets, left_on='receiver', right_on='address', how='left')
    receiver = receiver.rename(columns = {"owner": "receiver_name", "category":"receiver_category"}) 
    receiver = receiver.drop(['address'], axis=1)
    
    tnx_labeled = dd.merge(sender, receiver,  how='inner', on=['hash', 'block_timestamp', 'sender','receiver', 'date', 'btc', 'dollar', 'percent_marketcap', 'PriceUSD'])
    return tnx_labeled
