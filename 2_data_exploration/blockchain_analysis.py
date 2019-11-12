# -*- coding: utf-8 -*-
"""
Created on Sat Nov  9 12:33:29 2019

@author: David

https://www.kaggle.com/ibadia/bitcoin-101-bitcoins-and-detailed-insights
https://www.kaggle.com/sohier/tracing-the-10-000-btc-pizza
"""





'''
df = df[df['owner'] == 'unknown']
df = df.fillna('2020-01')
df['monthly'] = pd.to_datetime(df['last_out']).apply(lambda x: '{year}-{month}'.format(year=x.year, month=x.month))

df['monthly'] =pd.to_datetime(df.monthly, format = '%Y/%m/%d')
df = df.sort_values(by='monthly') 

tmp = df[['ranking', 'monthly']]
res = tmp.groupby('monthly').count()

res.plot()
'''