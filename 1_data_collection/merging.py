# -*- coding: utf-8 -*-
"""
Created on Mon Nov 11 18:33:32 2019

@author: David
"""

#https://coinmetrics.io/introducing-adjusted-estimates/

#pd.set_option('display.max_columns', None)

import pandas as pd

#import
btc_price_data = pd.read_csv("data/btc.csv") #https://coinmetrics.io/community-data-dictionary/
tnx = pd.read_csv("data/transactions_500BTC_2.csv")
wallets_1 = pd.read_csv("data/wallets_walletexplorer.csv", index_col=False)
wallets_2 = pd.read_csv("data/wallets_bitinfocharts.csv", index_col=False)


#Preprocess transactions
#tnx['date'] = pd.to_datetime(tnx['timestamp'], unit='ms' )
#tnx['btc'] = tnx['satoshis'].apply(lambda x: x/100000000)
tnx['date'] = pd.to_datetime(tnx['block_timestamp']).apply(lambda x: '{year}-{month}-{day}'.format(year=x.year, month=x.month, day=x.day))


#Merge with wallets
wallets = wallets_1[['address', 'owner', 'category']]
wallets = wallets.append(wallets_2.dropna())
wallets = wallets.drop_duplicates(subset='address', keep='last')

sender = pd.merge(tnx, wallets, left_on='sender', right_on='address', how='left')
sender.rename(columns = {"owner": "sender_name", 
                         "category":"sender_category"
                         }, inplace = True) 
columns = ['address']
sender = sender.drop(columns, axis=1)

receiver = pd.merge(tnx, wallets, left_on='receiver', right_on='address', how='left')
receiver.rename(columns = {"owner": "receiver_name", 
                         "category":"receiver_category"
                         }, inplace = True) 
columns = ['address']
receiver = receiver.drop(columns, axis=1)

tnx_labeled = pd.merge(sender, receiver,  how='inner', on=['hash', 'block_timestamp', 'sender','receiver', 'date', 'value'])
tnx_labeled.rename(columns = {"value": "btc"}, inplace = True) 

#Merge with price data
btc_price_data = btc_price_data[['date', 'CapMrktCurUSD','PriceUSD']]
btc_price_data['date'] = pd.to_datetime(btc_price_data['date']).apply(lambda x: '{year}-{month}-{day}'.format(year=x.year, month=x.month, day=x.day))

data = pd.merge(tnx_labeled, btc_price_data, on='date', how='inner')
#data.set_index('date', inplace=True)
#data = data.sort_values('date', axis=0, ascending=True)
data['dollar'] = data['btc'] * data['PriceUSD']
data['percent_marketcap'] = (data['dollar'] / data['CapMrktCurUSD']) *100

#filter
final = data[data['dollar'] >= 10000000]
#tmp = data[data['percent_marketcap'] >= 0.5]

#export trainingset
final.to_csv("transactions_10MIO.csv", index=False)




#testing
#a = max(data.date)
#tnx.columns.values
#tmp = tnx.head(1000)






'''
#https://stackoverflow.com/questions/15222754/groupby-pandas-dataframe-and-select-most-common-value
tmp = tnx_labeled[['timestamp','btc', 'sender_name']]
tmp = tmp.dropna()
tmp = tmp.groupby(['timestamp','btc']).agg(lambda x:x.value_counts().index[0])
tmp = tmp.groupby(['timestamp','btc'])['sender_name'].agg(pd.Series.mode).to_frame()
'''



'''
#All Sender
#All Receiver

#Label Sender and Receiver
#
#Append and get unique Sender and Receiver
#Get 

Group by transaction_id (sum, category)


'''

