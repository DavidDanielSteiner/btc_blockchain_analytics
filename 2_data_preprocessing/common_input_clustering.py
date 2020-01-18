# -*- coding: utf-8 -*-
"""
Created on Mon Nov 25 16:44:34 2019

@author: David
"""

#https://en.bitcoin.it/wiki/Common-input-ownership_heuristic

import pandas as pd
import random
import string


def randomString(x):
    """Generate a random string of fixed length """
    stringLength=2
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))


def check_owner(owner):  
    if not isinstance(owner, str):
        owner = sorted(owner, key=len)
        owner = owner[-1]
    
    length = len(owner)
    if length == 2:
        return 'unknown'
    else:
        return owner


def remove_conflicting_tx(row):
    if not isinstance(row['owner'], str):
        return 'conflict'
    else:
        return row['owner']
        
    
def aggregate_most_common(df):
    df = df[['hash', 'owner']]
    tmp = df[df['owner'].isna()]
    tmp['owner'] = tmp['owner'].apply(randomString)
    df = df.dropna()
    df = df.append(tmp)
    df = df.groupby(['hash'], as_index=False)['owner'].agg(pd.Series.mode) #https://stackoverflow.com/questions/15222754/groupby-pandas-dataframe-and-select-most-common-value
    df['owner'] = df['owner'].apply(check_owner)
    
    #tmp = df[['hash', 'owner']]
    #tmp = df.groupby(['hash'], as_index=False)['owner'].agg(pd.Series.mode)  
    #tmp['owner'] = tmp.apply(remove_conflicting_tx, axis=1)
    #conflicts = tmp[tmp['owner'] == 'conflict']
    #df = tmp[tmp['owner'] != 'conflict']
    #print("Removed " + str(len(conflicts)))
    return df


def group_transactions(df, unique=False):
    sender = df[['hash', 'sender_name']]
    sender.rename(columns = {"sender_name" : 'owner'}, inplace = True) 
    sender = aggregate_most_common(sender)
    sender.rename(columns = {"owner" : 'sender_name_new'}, inplace = True) 
    
    #receiver = df[['hash', 'receiver_name']]
    #receiver.rename(columns = {"receiver_name" : 'owner'}, inplace = True) 
    #receiver = aggregate_most_common(receiver)
    #receiver.rename(columns = {"owner" : 'receiver_name2'}, inplace = True) 
    
    if unique:
        df = df.drop_duplicates(subset='hash', keep='last')
        
    df_grouped = pd.merge(df, sender, on="hash", how="inner")
    #df_grouped = df_grouped.merge(receiver, on="hash", how="inner")
    return df_grouped


def regroup(df):
    sender = df[['sender', 'sender_name_new']]
    sender = sender[sender['sender_name_new'] != 'unknown']

    sender.rename(columns = {"sender_name_new" : 'owner',
                          "sender" : 'address'}, inplace = True) 
    
    df = sender.drop_duplicates() 
    return df


def add_category(wallets, labeled_tnx):
    wallet_owners = wallets[['owner', 'category']].drop_duplicates(subset='owner', keep='last').reset_index(drop=True)
    
    sender = pd.merge(labeled_tnx, wallet_owners, left_on='sender_name', right_on='owner', how='left')    
    sender = sender.drop(['receiver_name', 'sender_name'], axis=1)    
    sender.rename(columns = {"owner": "sender_name", "category":"sender_category"}, inplace = True) 

    receiver = pd.merge(labeled_tnx, wallet_owners, left_on='receiver_name', right_on='owner', how='left')
    receiver = receiver.drop(['sender_name', 'receiver_name'], axis=1)
    receiver.rename(columns = {"owner": "receiver_name", "category":"receiver_category"}, inplace = True) 
    
    tnx_category = pd.merge(sender, receiver,  how='inner', on=['hash', 'block_timestamp', 'sender','receiver', 'date', 'btc', 'dollar', 'percent_marketcap', 'PriceUSD'])
    tnx_category = tnx_category[['hash', 'block_timestamp', 'sender', 'receiver', 'btc', 'dollar', 'PriceUSD', 'percent_marketcap', 'sender_name', 'sender_category', 'receiver_name', 'receiver_category']]    
    return tnx_category

    
def merge_tnx_wallets(tnx, wallets_subset, labeled_wallets):
    #Merge trnsactions with wallet labels
    wallets = pd.concat([wallets_subset,labeled_wallets]).drop_duplicates(subset='address').reset_index(drop=True)
    wallets = wallets[['address', 'owner']]
    
    sender = pd.merge(tnx, wallets, left_on='sender', right_on='address', how='left')
    sender.rename(columns = {"owner": "sender_name"}, inplace = True) 
    sender = sender.drop(['address'], axis=1)    
    
    receiver = pd.merge(tnx, wallets, left_on='receiver', right_on='address', how='left')
    receiver.rename(columns = {"owner": "receiver_name"}, inplace = True) 
    receiver = receiver.drop(['address'], axis=1)
    
    tnx_labeled = pd.merge(sender, receiver,  how='inner', on=['hash', 'block_timestamp', 'sender','receiver', 'date', 'btc', 'dollar', 'percent_marketcap', 'PriceUSD']) 
    return tnx_labeled

