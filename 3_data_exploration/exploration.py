# -*- coding: utf-8 -*-
"""
Created on Mon Nov 18 20:57:26 2019

@author: David

https://towardsdatascience.com/data-science-43c246d4eebc
https://www.shanelynn.ie/summarising-aggregation-and-grouping-data-in-python-pandas/
https://dfrieds.com/data-analysis/groupby-python-pandas
"""

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# =============================================================================
# LABELS
# =============================================================================

wallets = pd.read_csv("data/wallets.csv", index_col=False)

sns.countplot(x='category', data=wallets) 

plt.figure(figsize = (15,40))
order = wallets['address'].value_counts(ascending=False).index
sns.countplot(y='owner', data=wallets, order = order) 



# =============================================================================
# Transactions                
# =============================================================================

#filter_tnx = tnx[tnx['hash'] == '55454a47565f17cb29d96a78645ed44e089d4701a1d6c1c537441a2b205c9edf']

tnx = pd.read_csv("data/transactions_10MIO_3.csv", index_col=False)
tnx["btc"] = tnx["btc"].astype(int)
tnx["dollar"] = tnx["dollar"].astype(int)
tnx = tnx.fillna('unknown')    
#tnx['date'] = pd.to_datetime(tnx['block_timestamp']) 
tnx['block_timestamp'] = pd.to_datetime(tnx['block_timestamp']) 
tnx['date'] = pd.to_datetime(tnx['block_timestamp']).apply(lambda x: '{year}-{month}-{day}'.format(year=x.year, month=x.month, day=x.day))   
#tnx.sort_values(by=['date'], inplace=True, ascending=True)

tnx.dtypes
#sen = tnx[ (tnx['receiver_category'] == 'Exchange') | (tnx['sender_category'] == 'Exchange') ]
#sen = tnx[tnx['sender_category'] != 'Exchange']


'''transactions with single sender / self transactions'''
#How many senders and receivers per transaction
tmp = tnx.groupby(['hash']).nunique()
sns.boxplot(x=tmp["sender"])

#self transactions
tmp = tnx[tnx.groupby('hash')['sender'].transform('size') == 1]
self_transactions = tmp[tmp['sender'] == tmp['receiver']] #43484 self transactions

sns.countplot(x='receiver_category', data=self_transactions) 
sns.countplot(y='receiver_name', data=self_transactions, order= self_transactions['receiver_name'].value_counts(ascending=False).index) 
sns.boxplot(x=self_transactions["dollar"], y=self_transactions["receiver_name"], showfliers=False, order = self_transactions.groupby("receiver_name")["dollar"].median().fillna(0).sort_values()[::-1].index) 

#Scatterplot (Date, Dollar, BTC)
plt.figure(figsize = (20,20))
plt.xlim(self_transactions['block_timestamp'].min(), self_transactions['block_timestamp'].max())
cmap = sns.cubehelix_palette(dark=.3, light=.8, as_cmap=True)
ax = sns.scatterplot(x="block_timestamp", y="dollar",
                      hue="receiver_category", size="btc",
                      palette="Set2",
                      data=self_transactions)



'''grouped transactions without self transactions'''
tnx_grouped_all = tnx.groupby(['hash'], as_index=False).first()
tnx_grouped = pd.concat([tnx_grouped_all, self_transactions]).drop_duplicates(keep=False)

#General Information
sns.countplot(x='receiver_category', data=tnx_grouped) 
ax = sns.boxplot(x=tnx_grouped["receiver_category"], y=tnx_grouped["dollar"], showfliers=False) 

fig, ax1 = plt.subplots(figsize=(20,10))
graph = sns.countplot(ax=ax1,x='receiver_name', data=tnx_grouped, order= tnx_grouped['receiver_name'].value_counts(ascending=False).index)
graph.set_xticklabels(graph.get_xticklabels(),rotation=90)
for p in graph.patches:
    height = p.get_height()
    graph.text(p.get_x()+p.get_width()/2., height + 0.1,height ,ha="center")


#Distribution of transactions dollar value
plt.figure(figsize = (10,10))
sns.boxplot(x=tnx_grouped["dollar"], y=tnx_grouped["receiver_name"], order = tnx_grouped.groupby("receiver_name")["dollar"].median().fillna(0).sort_values()[::-1].index) 
plt.figure(figsize = (10,10))
sns.boxplot(x=tnx_grouped["dollar"], y=tnx_grouped["receiver_name"], showfliers=False, order = tnx_grouped.groupby("receiver_name")["dollar"].median().fillna(0).sort_values()[::-1].index) 

#Distribution of transactions market cap value
plt.figure(figsize = (10,10))
sns.boxplot(x=tnx_grouped["percent_marketcap"], y=tnx_grouped["receiver_name"], order = tnx_grouped.groupby("receiver_name")["percent_marketcap"].median().fillna(0).sort_values()[::-1].index) 
sns.boxplot(x=tnx_grouped["receiver_category"], y=tnx_grouped["percent_marketcap"], showfliers=False)

#Scatterplot (Date, Dollar, BTC)
plt.figure(figsize = (20,20))
#plt.xlim(tnx_grouped['block_timestamp'].min(), tnx_grouped['block_timestamp'].max())
plt.xlim(pd.to_datetime('2017-08-01 00:00:00+00:00'), tnx_grouped['block_timestamp'].max())
cmap = sns.cubehelix_palette(dark=.3, light=.8, as_cmap=True)
ax = sns.scatterplot(x="block_timestamp", y="dollar",
                      hue="receiver_category", size="btc",
                      palette="Set2",
                      data=tnx_grouped)



'''addresses'''
tmp1 = tnx.groupby(['receiver']).agg({'hash':['count', 'nunique', lambda x: x.count() / x.nunique()],
                         'receiver_name':'first'})

#unique_tnx = tnx.drop_duplicates(subset='hash', keep='last')
tmp2 = tnx_grouped.groupby(['receiver']).agg({'hash':'count',
                         'block_timestamp':['first', 'last'], 
                         'receiver_name':'first',
                         'receiver_category':'first',
                         'sender_name':lambda x:x.value_counts().index[0],
                         'dollar': ['sum', 'mean', 'median', 'max']})                   

tmp1.columns = ['adr_total', 'txns', 'adr_per_tnx', 'x']                  
tmp2.columns = ['xx', 'first_tnx', 'last_tnx', 'receiver', 'category', 'sender', 'dollar_sum', 'dollar_mean', 'dollar_median', 'dollar_max']
tmp3 = tmp1.join(tmp2, how='inner')
tmp3 = tmp3.drop_duplicates(keep='last')

tmp3 = tmp3.reset_index(drop=True)
tmp3.to_csv("cluster_data.csv", index='False')

tmp4 = tmp3[(tmp3['adr_per_tnx'] <= 100 ) & (tmp3['dollar_median'] < 20000000)] 
tmp5 = tmp3[(tmp3['dollar_median'] <= 20000000)] 
#tmp3 = tmp3.dropna(subset=['receiver', 'x'])
cluster = tmp3[['adr_per_tnx', 'dollar_meadian']]
#cluster = tmp4[['adr_total', 'txns', 'adr_per_tnx','dollar_sum', 'dollar_mean','dollar_meadian', 'dollar_max']]

plt.figure(figsize = (20,20))
sns.scatterplot(x="adr_per_tnx", y="dollar_median",
                     hue="category", size="dollar_sum",
                     data=tmp3)


ax = sns.countplot(y="category", data=tmp3)
total = len(tmp3['category'])
for p in ax.patches:
        percentage = '{:.1f}%'.format(100 * p.get_width()/total)
        x = p.get_x() + p.get_width() + 0.02
        y = p.get_y() + p.get_height()/2
        ax.annotate(percentage, (x, y))
plt.title('total tnx per category')
plt.show()

tmp = tmp3[(tmp3['adr_per_tnx'] <= 1 ) & (tmp3['dollar_median'] < 20000000)] 
ax = sns.countplot(y="category", data=tmp)
total = len(tmp['category'])
for p in ax.patches:
        percentage = '{:.1f}%'.format(100 * p.get_width()/total)
        x = p.get_x() + p.get_width() + 0.02
        y = p.get_y() + p.get_height()/2
        ax.annotate(percentage, (x, y))
plt.title('Tnx with adr_per_tnx = 1 and dollar_median <20000000')
plt.show()


'''cluster'''
import sklearn
import numpy as np

# Convert DataFrame to matrix
mat = cluster.values
# Using sklearn
km = sklearn.cluster.KMeans(n_clusters=5)
km.fit(mat)
# Get cluster assignment labels
labels = km.labels_
# Format results as a DataFrame
#results = pd.DataFrame([tmp3.index,labels]).T
#res = results.join(tmp3)

tmp4['knn'] = labels

plt.figure(figsize = (10,10))
sns.boxplot(x=tmp4["knn"], y=tmp4["dollar_meadian"]) 

unk = tmp4[tmp4['receiver'] == 'unknown']
ex = tmp4[tmp4['receiver'] != 'unknown']

sns.countplot(x='knn', data=unk) 
sns.countplot(x='knn', data=ex) 



'''categorize by transactions type'''
#tmp = tnx_grouped[['block_timestamp', 'date', 'dollar', 'sender_name', 'receiver_name']]
tmp = tnx_grouped[['date', 'dollar', 'sender_name', 'receiver_name', 'sender_category', 'receiver_category']]
exchange_exchange = tmp[(tmp['sender_category'] == 'Exchange') &  (tmp['receiver_category'] == 'Exchange')]
other_exchange = tmp[(tmp['sender_category'] != 'Exchange') &  (tmp['receiver_category'] == 'Exchange')]
exchange_other = tmp[(tmp['sender_category'] == 'Exchange') &  (tmp['receiver_category'] != 'Exchange')]
other_other = tmp[(tmp['sender_category'] != 'Exchange') &  (tmp['receiver_category'] != 'Exchange')]


date_start = '2018-01-01'
date_end = '2018-05-01'
all_days = pd.date_range(date_start, date_end, freq='D')

def prepare_for_plot(df, category):
    df['date'] = pd.to_datetime(df['date'])
    df = df.groupby('date').sum()
    #df = df[df['dollar'] < 50000000000] #remove outliers
    df['category'] = category    
    df = df.reindex(all_days)
    #df.plot(figsize=(16, 8))
    return df
    
exchange_exchange = prepare_for_plot(exchange_exchange, 'exchange_exchange')
other_exchange = prepare_for_plot(other_exchange, 'unknown_exchange')
exchange_other = prepare_for_plot(exchange_other, 'exchange_unknown')
other_other = prepare_for_plot(other_other, 'unknown_unkown')
all_tnx = prepare_for_plot(tmp, 'all')

tnx_category = pd.concat([exchange_exchange, other_exchange, exchange_other, other_other])
    
price = pd.read_csv("data/btc_price_data.csv") #https://coinmetrics.io/community-data-dictionary/   #https://coinmetrics.io/newdata/btc.csv
price['date'] = pd.to_datetime(price['date'])
price.set_index('date', inplace=True)
price = price[['PriceUSD']]
price['return'] = price.pct_change(1) * 100
price_2= pd.read_csv("data/GDAX.csv") #https://www.cryptodatadownload.com/data/northamerican/
price_2['volatility'] = (1 - (price_2['Low'] / price_2['High'])) * 100
price_2['date'] = pd.to_datetime(price_2['Date'])
price_2.set_index('date', inplace=True)   
price = price.join(price_2)
price = price.loc[date_start:date_end]


plt.figure(figsize=(30,20))
price_top = plt.subplot2grid((10,4), (0, 0), rowspan=2, colspan=4)
volatility = plt.subplot2grid((10,4), (2, 0), rowspan=1, colspan=4)
change_daily = plt.subplot2grid((10,4), (3, 0), rowspan=1, colspan=4)
tnx_vol_1 = plt.subplot2grid((10,4), (4,0), rowspan=1, colspan=4)
tnx_vol_2 = plt.subplot2grid((10,4), (5,0), rowspan=1, colspan=4)
tnx_vol_3 = plt.subplot2grid((10,4), (6,0), rowspan=1, colspan=4)
tnx_vol_4 = plt.subplot2grid((10,4), (7,0), rowspan=1, colspan=4)

volatility.plot(price.index, price['volatility']) 
change_daily.axhline(y=0, color='r', linestyle='-')
price_top.plot(price.index, price['PriceUSD']) 
tnx_vol_1.bar(other_exchange.index, other_exchange['dollar']) 
tnx_vol_2.bar(exchange_other.index, exchange_other['dollar']) 
tnx_vol_3.bar(other_other.index, other_other['dollar']) 
tnx_vol_4.bar(exchange_exchange.index, exchange_exchange['dollar']) 
 
price_top.axes.get_xaxis().set_visible(False)
price_top.set_title('BTC transactions per category')
price_top.set_ylabel('Closing Price')
volatility.set_ylabel('Daily Volatility')
change_daily.set_ylabel('Daily Return')
tnx_vol_1.set_ylabel('tnx other_exchange')
tnx_vol_2.set_ylabel('tnx exchange_other')
tnx_vol_3.set_ylabel('tnx other_other')
tnx_vol_4.set_ylabel('tnx exchange_exchange')
    


#scatterplot by transaction type
tmp = tnx_category.reset_index().dropna()
tmp.rename(columns = {"index": "date"}, inplace = True) 
plt.figure(figsize = (20,20))
plt.xlim(pd.to_datetime('2015-01-01 00:00:00+00:00'), tmp['date'].max())
ax = sns.scatterplot(x="date", y="dollar",
                      hue="category",
                      palette="Set1",
                      alpha=.5, 
                      data=tmp)

#scatterplots by transaction type
sns.set(style="ticks", color_codes=True)
g = sns.FacetGrid(tmp, col="category", palette="GnBu_d", size=5, aspect=1.5)
g.map(plt.scatter, "date", "dollar", alpha=.1)
g.add_legend()







'''
--Seaborn---

sns.set(style="whitegrid")
ax = sns.barplot(x="category", data=wallets, palette="Blues_d")
ax = sns.boxplot(x=tnx_grouped["receiver_category"], y=tnx_grouped["dollar"], showfliers=False) #boxplot
ax = sns.barplot(x="receiver_category", y='dollar', data=tnx_grouped) #barplot
sns.distplot(tmp) #histogram
sns.countplot(x='receiver_category', data=tnx_grouped) #countplot

#Scatterplot: https://seaborn.pydata.org/generated/seaborn.scatterplot.html
>>> ax = sns.scatterplot(x="total_bill", y="tip",
...                      hue="size", size="size",
...                      data=tips)


plt.figure(figsize = (20,15))
plt.xticks(rotation=45)
ax = sns.countplot(x='receiver_name', data=tnx_grouped, order = tnx_grouped.receiver_name.value_counts().index) 

plt.title('Distribution of  Configurations')
plt.xlabel('Number of Axles')

#self_transactions = pd.merge(tmp2, tnx, left_index=True, right_on='hash')
#self_transactions = tnx.drop_duplicates('hash', keep='last')
'''

date_start = '2017-11-01'
date_end = '2018-04-01'

all_days = pd.date_range(min(tnx['date']), max(tnx['date']), freq='D')
df = df.reindex(all_days)


def prepare_for_plot(df, category):
    df = df.groupby('date').sum()
    df = df[df['dollar'] < 50000000000] #remove outliers
    df['category'] = category    
    
    #df.plot(figsize=(16, 8))
    return df







# =============================================================================
# Price Data
# =============================================================================




