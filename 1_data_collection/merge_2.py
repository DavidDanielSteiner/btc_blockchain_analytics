# -*- coding: utf-8 -*-
"""
Created on Tue Nov 12 15:30:54 2019

@author: David
"""

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
tnx = pd.read_csv("data/transactions_500BTC_1.csv")
outputs = pd.read_csv("data/outputs_500BTC.csv")
inputs = pd.read_csv("data/inputs_500BTC.csv")


#tnx = pd.merge(outputs, inputs,  how='inner', on=['transaction_hash', 'timestamp'])


#Preprocess transactions
#tnx['date'] = pd.to_datetime(tnx['timestamp'], unit='ms' )
tnx['date'] = pd.to_datetime(tnx['timestamp']).apply(lambda x: '{year}-{month}-{day}'.format(year=x.year, month=x.month, day=x.day))
tnx['btc'] = tnx['satoshis'].apply(lambda x: x/100000000)
tnx = tnx[['transaction_hash', 'date', 'timestamp', 'address', 'btc',]]




#Merge with wallets
wallets = wallets_1[['address', 'owner', 'category']]
wallets = wallets.append(wallets_2.dropna())
wallets = wallets[wallets['address'].unique()]

'''
wallets_1 = pd.read_csv("data/wallets_walletexplorer.csv", index_col=False)
wallets_2 = pd.read_csv("data/wallets_bitinfocharts.csv", index_col=False)

receiver = pd.merge(outputs, wallets, on='address', how='left')
receiver.rename(columns = {"owner": "receiver_name", 
                         "category":"receiver_category",
                         "satoshis" : "receiver_satoshis",
                         "address" : "receiver_address"
                         }, inplace = True) 

sender = pd.merge(inputs, wallets, on='address', how='left')
sender.rename(columns = {"owner": "sender_name", 
                         "category":"sender_category",
                         "satoshis" : "sender_satoshis",
                         "address" : "sender_address"
                         }, inplace = True) 
'''


sender = pd.merge(tnx, wallets, left_on='sender', right_on='address', how='inner')
sender.rename(columns = {"owner": "sender_name", 
                         "category":"sender_category"
                         }, inplace = True) 
columns = ['address', 'satoshis']
sender = sender.drop(columns, axis=1)

receiver = pd.merge(tnx, wallets, left_on='receiver', right_on='address', how='inner')
receiver.rename(columns = {"owner": "receiver_name", 
                         "category":"receiver_category"
                         }, inplace = True) 
columns = ['address', 'satoshis']
receiver = receiver.drop(columns, axis=1)











#Merge with price data
btc_price_data = btc_price_data[['date', 'CapMrktCurUSD','PriceUSD', 'TxCnt', 'TxTfrValAdjUSD']]
btc_price_data['date'] = pd.to_datetime(btc_price_data['date']).apply(lambda x: '{year}-{month}-{day}'.format(year=x.year, month=x.month, day=x.day))

data = pd.merge(tnx_labeled, btc_price_data, on='date', how='inner')
data.set_index('date', inplace=True)
data = data.sort_values('date', axis=0, ascending=True)
data['dollar'] = data['btc'] * data['PriceUSD']
data['percent_marketcap'] = (data['dollar'] / data['CapMrktCurUSD']) *100

#filter
tmp = data[data['dollar'] >= 10000000]
tmp = data[data['percent_marketcap'] >= 0.001]

#export trainingset


#testing
a = max(data.date)
tnx.columns.values
tmp = wallets.head(1000)









'''
#All Sender
#All Receiver

#Label Sender and Receiver
#
#Append and get unique Sender and Receiver
#Get 

Group by transaction_id (sum, category)


'''





