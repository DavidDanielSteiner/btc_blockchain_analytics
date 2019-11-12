# -*- coding: utf-8 -*-
"""
Created on Tue Nov 12 17:24:08 2019

@author: David
"""

import pandas as pd
import random
import string


df = pd.read_csv("data/transactions_10MIO.csv", index_col = False)

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




def randomString(x):
    """Generate a random string of fixed length """
    stringLength=20
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))

def check_owner(x):    
    if not isinstance(x, str):
        x = sorted(x, key=len)
        x = x[0]
    
    length = len(x)
    if length == 20:
        return 'unknown'
    else:
        return x

def group_transactions(df):
    df = df[['hash', 'owner']]
    tmp = df[df['owner'].isna()]
    tmp['owner'] = tmp['owner'].apply(randomString)
    df = df.dropna()
    df = df.append(tmp)
    df = df.groupby(['hash'], as_index=False)['owner'].agg(pd.Series.mode) #https://stackoverflow.com/questions/15222754/groupby-pandas-dataframe-and-select-most-common-value
    df['owner'] = df['owner'].apply(check_owner)
    return df

sender = df[['hash', 'sender_name']]
sender.rename(columns = {"sender_name" : 'owner'}, inplace = True) 
sender = group_transactions(sender)
sender.rename(columns = {"owner" : 'sender_name2'}, inplace = True) 

receiver = df[['hash', 'receiver_name']]
receiver.rename(columns = {"receiver_name" : 'owner'}, inplace = True) 
receiver = group_transactions(receiver)
receiver.rename(columns = {"owner" : 'receiver_name2'}, inplace = True) 

tmp = df.drop_duplicates(subset='hash', keep='last')
tmp = tmp.merge(sender, on="hash", how="inner")
tmp = tmp.merge(receiver, on="hash", how="inner")




#x = sender.append(receiver).drop_duplicates(subset='hash', keep='last')
#y = df.drop_duplicates(subset='hash', keep='last')

m = tmp.drop_duplicates(subset='owner', keep='last')
x = tmp[(tmp['sender_name2'] != 'unknown') &  (tmp['receiver_name2'] == 'unknown')]
#y = receiver[receiver['owner'] == 'F2Pool']


