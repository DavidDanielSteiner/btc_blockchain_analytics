# -*- coding: utf-8 -*-
"""
Created on Sat Nov  9 12:33:29 2019

@author: David

https://www.kaggle.com/ibadia/bitcoin-101-bitcoins-and-detailed-insights
https://www.kaggle.com/sohier/tracing-the-10-000-btc-pizza



df = df[df['owner'] == 'unknown']
df = df.fillna('2020-01')
df['monthly'] = pd.to_datetime(df['last_out']).apply(lambda x: '{year}-{month}'.format(year=x.year, month=x.month))

df['monthly'] =pd.to_datetime(df.monthly, format = '%Y/%m/%d')
df = df.sort_values(by='monthly') 

tmp = df[['ranking', 'monthly']]
res = tmp.groupby('monthly').count()

res.plot()
"""






'''MT GOX'''
import numpy as np
filtered_transactions = filtered_transactions.replace(np.nan, 'unknown', regex=True)
other_other = df_grouped[(df_grouped['sender_name2_x'] == 'unknown') &  (df_grouped['receiver_name2_x'] == 'unknown')]

tmp = df_grouped[df_grouped['sender_name'] =='Mt.Gox']
tmp = df_grouped[df_grouped['hash'] =='2beb7a87c73addffa0ed699bd324957991ff92f5f127de2e60240008cace8d93']
tmp = df_grouped[df_grouped['hash'] =='7310efd96d4027707f5b3eb38cd7611fe90c85466f63f8d03469eead905ed1c3']
tmp = df_grouped[df_grouped['hash'] =='55454a47565f17cb29d96a78645ed44e089d4701a1d6c1c537441a2b205c9edf']
tmp = df_grouped[df_grouped['hash'] =='3bb69b7b847f1abbab86644c46ccd6d1f48b0f2b68bb51ce4cf2bee69454e86d']
tmp = df_grouped[df_grouped['hash'] =='fa824de460a4048fdc404b041bfad6ae5c689ec196721f1eb7ae6c439961df86']
#testing
#a = max(data.date)
#tnx.columns.values
#tmp = tnx.head(1000)




tmp = tmp[['date', 'dollar', 'sender_name2', 'receiver_name2']]
exchange_exchange = tmp[(tmp['sender_name2'] != 'unknown') &  (tmp['receiver_name2'] != 'unknown')]
other_exchange = tmp[(tmp['sender_name2'] == 'unknown') &  (tmp['receiver_name2'] != 'unknown')]
exchange_other = tmp[(tmp['sender_name2'] != 'unknown') &  (tmp['receiver_name2'] == 'unknown')]
other_other = tmp[(tmp['sender_name2'] == 'unknown') &  (tmp['receiver_name2'] == 'unknown')]



date_start = '2017-11-01'
date_end = '2018-04-01'
all_days = pd.date_range(date_start, date_end, freq='D')

def prepare_for_plot(df, category):
    df = df.groupby('date').sum()
    df = df[df['dollar'] < 50000000000] #remove outliers
    df['category'] = category    
    df = df.reindex(all_days)
    #df.plot(figsize=(16, 8))
    return df
    
exchange_exchange = prepare_for_plot(exchange_exchange, 'exchange')
other_exchange = prepare_for_plot(other_exchange, 'to')
exchange_other = prepare_for_plot(exchange_other, 'to')
other_other = prepare_for_plot(other_other, 'to')
all_tnx = prepare_for_plot(tmp, 'to')

    
price = pd.read_csv("data/btc.csv")
price['date'] = pd.to_datetime(price['date'])
price.set_index('date', inplace=True)
price = price[['PriceUSD']]
price = price.loc[date_start:date_end]
price['return'] = price.pct_change(1) * 100






plt.figure(figsize=(30,20))

top1 = plt.subplot2grid((7,4), (2, 0), rowspan=1, colspan=4)
top = plt.subplot2grid((7,4), (0, 0), rowspan=2, colspan=4)
bottom = plt.subplot2grid((7,4), (3,0), rowspan=1, colspan=4)
bottom2 = plt.subplot2grid((7,4), (4,0), rowspan=1, colspan=4)
bottom3 = plt.subplot2grid((7,4), (5,0), rowspan=1, colspan=4)
bottom4 = plt.subplot2grid((7,4), (6,0), rowspan=1, colspan=4)

top1.plot(price.index, price['return']) #CMT.index gives the dates
top1.axhline(y=0, color='r', linestyle='-')
top.plot(price.index, price['PriceUSD']) #CMT.index gives the dates
bottom.bar(other_exchange.index, other_exchange['dollar']) 
bottom2.bar(exchange_other.index, exchange_other['dollar']) 
bottom3.bar(other_other.index, other_other['dollar']) 
bottom4.bar(exchange_exchange.index, exchange_exchange['dollar']) 
 

# set the labels
top.axes.get_xaxis().set_visible(False)
top.set_title('Bitcoin Price')
top.set_ylabel('Adj Closing Price')
bottom.set_ylabel('Volume')


'''
fig, ax = plt.subplots(figsize=(20, 8)) 
#exchange_exchange.plot(ax = ax,label='exchange') 
other_exchange.plot(ax = ax) 
exchange_other.plot(ax = ax) 
#other_other.plot(ax = ax) 
price.plot(ax = ax, secondary_y = True)
ax.legend(["exchange", "to", 'from', 'other', 'price'])


#all_txn = all_tnx.set_index('category', append=True).unstack().interpolate().plot(subplots=True)
fig, ax= plt.subplots()
for category in all_tnx['category'].unique():
    all_tnx[all_tnx['category'] == category]['dollar'].plot.line(ax=ax, label=category)
plt.legend()
'''