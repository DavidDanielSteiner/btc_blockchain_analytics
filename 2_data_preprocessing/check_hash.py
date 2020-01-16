# -*- coding: utf-8 -*-
"""
Created on Tue Nov 26 18:11:08 2019

@author: David
"""
import pandas as pd
df = pd.read_csv("../data/final_dataset/transactions_0.01_marketcap_2015.csv", index_col=False)
df = pd.read_csv("../data/transactions_100BTC_merged.csv", index_col=False)  

tmp = df[df['btc'] >= 1000]

df = df[['hash', 'sender_name']]
df = df.dropna()
df = tmp.groupby(['hash'], as_index=False)['sender_name'].agg(pd.Series.mode)

df.all()


df1 = tmp.groupby('hash')['sender_name'].apply(list).reset_index(name='sender_address')
