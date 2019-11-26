# -*- coding: utf-8 -*-
"""
Created on Tue Nov 26 18:11:08 2019

@author: David
"""
import pandas as pd

df = pd.read_csv("data/transactions_filtered_1MIO.csv", index_col=False)  
df = df[['hash', 'sender_name']]
df = df.dropna()
df = df.groupby(['hash'], as_index=False)['sender_name'].agg(pd.Series.mode)

df.all()


df1 = df.groupby('hash')['sender_name'].apply(list).reset_index(name='sender_address')
