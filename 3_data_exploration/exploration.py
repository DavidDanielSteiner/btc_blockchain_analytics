# -*- coding: utf-8 -*-
"""
Created on Mon Nov 18 20:57:26 2019

@author: David

"""

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import requests
import io

# =============================================================================
# LABELS
# =============================================================================
#wallets = pd.read_csv("data/wallets.csv", index_col=False)

addresses_known = pd.read_csv("../data/final_dataset/addresses_known_0.01_marketcap_2015.csv", index_col=False)
addresses_predicted = pd.read_csv("../data/final_dataset/addresses_predicted_0.01_marketcap_2015.csv", index_col=False)
wallets = addresses_known.append(addresses_predicted)

labeled_tnx = pd.read_csv("../data/transactions_100BTC_labeled.csv", index_col=False)
labeled_tnx = pd.read_csv("../data/transactions_100BTC_merged.csv", index_col=False)

#tmp = tmp.fillna('unknown')  

tmp = tmp.groupby(['sender_category']).agg(['count'], as_index=False).reset_index()
tmp = labeled_tnx.head()
known = addresses_known.groupby(['category']).agg(['count'], as_index=False).reset_index()
predicted = addresses_predicted.groupby(['category']).agg(['count'], as_index=False).reset_index()


#Addresses per category
fig, ax = plt.subplots()
graph = sns.countplot(x='category', data=wallets)
plt.title('Labeled addresses per category')
graph.set_xticklabels(graph.get_xticklabels())
for p in graph.patches:
    height = p.get_height()
    graph.text(p.get_x()+p.get_width()/2., height + 0.1,height ,ha="center")
plt.savefig('addresses_labeled_category.png', transparent=True)

#Addresses per owner
plt.figure(figsize = (15,40))
order = wallets['address'].value_counts(ascending=False).index
sns.countplot(y='owner', data=wallets, order = order) 

known = addresses_known.groupby(['category']).agg(['count'], as_index=False).reset_index()
predicted = addresses_predicted.groupby(['category']).agg(['count'], as_index=False).reset_index()

# =============================================================================
# Sankey diagram
# =============================================================================

from pySankey import sankey

'''
df.groupby(['hash'], as_index=False).agg(
        Mean=('n_sender', 'sum'), 
        Sum=('n_receiver', 'sum'),
        Unique = 'n_receiver', 'nunique')
        )
'''

#tnx = pd.read_csv("../data/transactions_100BTC_labeled_2.csv", index_col=False) #nrows = 1000000)

tnx = filtered_tnx
tnx = tnx.fillna('unknown') 
tnx = tnx[(tnx['sender_name'] != 'unknown') & (tnx['receiver_name'] != 'unknown')]
tnx.columns.values
tnx.dtypes
del tnx

#Sankey diagram per entity
grouped = tnx.groupby(['hash'], as_index=False)[ 'sender_name', 'receiver_name', 'sender_category', 'receiver_category'].agg(['min']).reset_index() #n_transactions = 6.92 mio
grouped.columns = ['hash', 'sender_name', 'receiver_name', 'sender_category', 'receiver_category']
df = grouped[['hash', 'sender_name', 'receiver_name']]
df = df.groupby(['sender_name', 'receiver_name'], as_index=False).agg(['count']).reset_index()
df.columns = ['sender_name', 'receiver_name', 'count']

df1 = df.sort_values(by=['count'], ascending=False).reset_index(drop=True)
df1=df1[df1['count'] >= 10]
sankey.sankey(
    left=df1['sender_name'], right=df1['receiver_name'], rightWeight=df1['count'],  leftWeight=df1['count'], aspect=5,
    fontsize=4, figure_name="sankey_100BTC_entity")


df2 = df[df['sender_name'] != df['receiver_name']]
df2 = df2.sort_values(by=['count'], ascending=False).reset_index(drop=True)
sankey.sankey(
    left=df2['sender_name'], right=df2['receiver_name'], rightWeight=df2['count'],  leftWeight=df2['count'], aspect=1,
    fontsize=4, figure_name="sankey_100BTC_entity_without_self_transactions")

df2 = df[(df['sender_name'] != 'unknown') & (df['receiver_name'] != 'unknown')]
df2 = df2.sort_values(by=['count'], ascending=False).reset_index(drop=True)
sankey.sankey(
    left=df2['sender_name'], right=df2['receiver_name'], rightWeight=df2['count'],  leftWeight=df2['count'], aspect=1,
    fontsize=4, figure_name="sankey_100BTC_entity_without_unknown")

df2 = df2[df2['sender_name'] != df2['receiver_name']]
df2 = df2.sort_values(by=['count'], ascending=False).reset_index(drop=True)
sankey.sankey(
    left=df2['sender_name'], right=df2['receiver_name'], rightWeight=df2['count'],  leftWeight=df2['count'], aspect=1,
    fontsize=4, figure_name="sankey_100BTC_entity_without_unknown_and_self_transactions")

df2= df2[df2['count'] >= 10]
df2 = df2.sort_values(by=['count'], ascending=False).reset_index(drop=True)
sankey.sankey(
    left=df2['sender_name'], right=df2['receiver_name'], rightWeight=df2['count'],  leftWeight=df2['count'], aspect=1,
    fontsize=4, figure_name="sankey_100BTC_entity_without_unknown_and_self_transactions_and_10")


#Sankey diagram per category
#df = tnx.groupby(['hash'], as_index=False)['sender_category', 'receiver_category'].agg(['min']).reset_index()
#df.columns = ['hash', 'sender_category', 'receiver_category']
df = grouped[['hash','sender_category', 'receiver_category']]
df = df.groupby(['sender_category', 'receiver_category'], as_index=False).agg(['count']).reset_index()
df.columns = ['sender_category', 'receiver_category', 'count']


df3 = df
df3 = df3.sort_values(by=['count'], ascending=False).reset_index(drop=True)
sankey.sankey(
    left=df3['sender_category'], right=df3['receiver_category'], rightWeight=df3['count'],  leftWeight=df3['count'], aspect=5,
    fontsize=6, figure_name="sankey_100BTC_category")

df4 = df[(df['sender_category'] != 'unknown') & (df['receiver_category'] != 'unknown')]
df4 = df4.sort_values(by=['count'], ascending=False).reset_index(drop=True)
sankey.sankey(
    left=df4['sender_category'], right=df4['receiver_category'], rightWeight=df4['count'],  leftWeight=df4['count'], aspect=5,
    fontsize=6, figure_name="sankey_100BTC_category_without_unknown")

df4 = df[df['sender_category'] != df['receiver_category']]
df4 = df4.sort_values(by=['count'], ascending=False).reset_index(drop=True)
sankey.sankey(
    left=df4['sender_category'], right=df4['receiver_category'], rightWeight=df4['count'],  leftWeight=df4['count'], aspect=5,
    fontsize=4, figure_name="sankey_100BTC_category_without_self_transactions")

df4 = df4[df4['sender_category'] != df4['receiver_category']]
df4 = df4.sort_values(by=['count'], ascending=False).reset_index(drop=True)
sankey.sankey(
    left=df4['sender_category'], right=df4['receiver_category'], rightWeight=df4['count'],  leftWeight=df4['count'], aspect=5,
    fontsize=4, figure_name="sankey_100BTC_category_without_unknown_and_self_transactions")


# =============================================================================
# Transactions                
# =============================================================================
def merge_tnx_wallets(tnx, wallets):
    tnx = tnx.drop(['sender_name', 'sender_category', 'receiver_name', 'receiver_category'], axis=1)
    wallets = wallets.drop_duplicates(subset='address', keep='last')
     
    sender = pd.merge(tnx, wallets, left_on='sender', right_on='address', how='left')
    sender.rename(columns = {"owner": "sender_name", "category":"sender_category"}, inplace = True) 
    sender = sender.drop(['address'], axis=1)    
    
    receiver = pd.merge(tnx, wallets, left_on='receiver', right_on='address', how='left')
    receiver.rename(columns = {"owner": "receiver_name", "category":"receiver_category"}, inplace = True) 
    receiver = receiver.drop(['address'], axis=1)
    
    tnx = pd.merge(sender, receiver,  how='inner', on=['hash', 'block_timestamp', 'sender','receiver', 'date', 'btc', 'dollar', 'percent_marketcap', 'PriceUSD'])
 
    return tnx

tnx = pd.read_csv("../data/final_dataset/transactions_0.01_marketcap_2015.csv", index_col=False)

#Preprocessing
tnx["btc"] = tnx["btc"].astype(int)
tnx["dollar"] = tnx["dollar"].astype(int)
#tnx = tnx.fillna('unknown')    
tnx['block_timestamp'] = pd.to_datetime(tnx['block_timestamp']) 
tnx['date'] = pd.to_datetime(tnx['block_timestamp']).apply(lambda x: '{year}-{month}-{day}'.format(year=x.year, month=x.month, day=x.day))   
#tnx.sort_values(by=['date'], inplace=True, ascending=True)
#tnx.dtypes
tnx = merge_tnx_wallets(tnx, wallets)



# =============================================================================
# Remove Self Transactions
# =============================================================================
'''transactions with single sender / self transactions'''
#How many senders per transaction
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
tnx_all = tnx.groupby(['hash'], as_index=False).first()
tnx_valid = pd.concat([tnx_all, self_transactions]).drop_duplicates(keep=False)
tnx_valid.to_csv("transactions_valid.csv")

#Total Transactions per category
fig, ax = plt.subplots()
graph = sns.countplot(x='receiver_category', data=tnx_all) 
plt.title('Receiver addresses of all transactions > 10mio US$')
graph.set_xticklabels(graph.get_xticklabels())
for p in graph.patches:
    height = p.get_height()
    graph.text(p.get_x()+p.get_width()/2., height + 0.1,height ,ha="center")
plt.savefig('transactions_category_all.png', transparent=True)

#Valid Transactions per category
fig, ax = plt.subplots()
graph = sns.countplot(x='receiver_category', data=tnx_valid) 
plt.title('Receiver addresses of valid transactions > 10mio US$ per category')
graph.set_xticklabels(graph.get_xticklabels())
for p in graph.patches:
    height = p.get_height()
    graph.text(p.get_x()+p.get_width()/2., height + 0.1,height ,ha="center")
plt.savefig('transactions_category_valid.png', transparent=True)


#Dollar value per category
ax = sns.boxplot(x=tnx_valid["receiver_category"], y=tnx_valid["dollar"], showfliers=False) 

#Receiver transactions per owner
fig, ax1 = plt.subplots(figsize=(20,10))
graph = sns.countplot(ax=ax1,x='receiver_name', data=tnx_valid, order= tnx_valid['receiver_name'].value_counts(ascending=False).index)
graph.set_xticklabels(graph.get_xticklabels(),rotation=90)
for p in graph.patches:
    height = p.get_height()
    graph.text(p.get_x()+p.get_width()/2., height + 0.1,height ,ha="center")


#Distribution of transactions dollar value
plt.figure(figsize = (10,10))
sns.boxplot(x=tnx_valid["dollar"], y=tnx_valid["receiver_name"], order = tnx_valid.groupby("receiver_name")["dollar"].median().fillna(0).sort_values()[::-1].index) 
plt.figure(figsize = (10,10))
sns.boxplot(x=tnx_valid["dollar"], y=tnx_valid["receiver_name"], showfliers=False, order = tnx_valid.groupby("receiver_name")["dollar"].median().fillna(0).sort_values()[::-1].index) 

#Distribution of transactions market cap value
plt.figure(figsize = (10,10))
sns.boxplot(x=tnx_valid["percent_marketcap"], y=tnx_valid["receiver_name"], order = tnx_valid.groupby("receiver_name")["percent_marketcap"].median().fillna(0).sort_values()[::-1].index) 
sns.boxplot(x=tnx_valid["receiver_category"], y=tnx_valid["percent_marketcap"], showfliers=False)

#Scatterplot (Date, Dollar, BTC)
plt.figure(figsize = (20,20))
#plt.xlim(tnx_valid['block_timestamp'].min(), tnx_valid['block_timestamp'].max())
plt.xlim(pd.to_datetime('2015-01-01 00:00:00+00:00'), tnx_valid['block_timestamp'].max())
cmap = sns.cubehelix_palette(dark=.3, light=.8, as_cmap=True)
ax = sns.scatterplot(x="block_timestamp", y="dollar",
                      hue="receiver_category", size="btc",
                      palette="Set2",
                      data=tnx_valid)


# =============================================================================
# Transaction type
# =============================================================================
'''categorize by transactions type'''

date_start = '2019-01-01'
date_end = '2020-01-01'
all_days = pd.date_range(date_start, date_end, freq='D')

def preprocess_transaction_types(df, category):
    df['date'] = pd.to_datetime(df['date'])
    df = df.groupby(['date']).sum()
    df = df.reindex(all_days)
    df['category'] = category     
    df = df.fillna(0)    
    return df


tnx_valid.columns.values
tmp = tnx_valid[['hash', 'date', 'dollar','percent_marketcap', 'sender_name', 'receiver_name', 'sender_category', 'receiver_category']]
all_all = preprocess_transaction_types(tmp, 'all')
exchange_exchange = preprocess_transaction_types(tmp[(tmp['sender_category'] == 'Exchange') &  (tmp['receiver_category'] == 'Exchange')], 'exc_exc')
other_exchange = preprocess_transaction_types(tmp[(tmp['sender_category'] != 'Exchange') &  (tmp['receiver_category'] == 'Exchange')], 'unk_exc')
exchange_other = preprocess_transaction_types(tmp[(tmp['sender_category'] == 'Exchange') &  (tmp['receiver_category'] != 'Exchange')], 'exc_unk')
other_other = preprocess_transaction_types(tmp[(tmp['sender_category'] != 'Exchange') &  (tmp['receiver_category'] != 'Exchange')], 'unk_unk')


'''
#Addresses per category  
tnx_category = pd.concat([exchange_exchange, other_exchange, exchange_other, other_other])
fig, ax = plt.subplots()
graph = sns.countplot(x='category', data=tnx_category)
plt.title('Transactions per transaction category')
graph.set_xticklabels(graph.get_xticklabels())
for p in graph.patches:
    height = p.get_height()
    graph.text(p.get_x()+p.get_width()/2., height + 0.1,height ,ha="center")
plt.savefig('transaction_types.png', transparent=True)



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

# =============================================================================
# Price data
# =============================================================================
def get_price_data():
    response=requests.get('https://coinmetrics.io/newdata/btc.csv').content
    price = pd.read_csv(io.StringIO(response.decode('utf-8')))
    price['date'] = pd.to_datetime(price['date'])
    price.set_index('date', inplace=True)
    price = price[['PriceUSD']]
    price['return'] = price.pct_change(1) * 100
    
    response=requests.get('https://www.cryptodatadownload.com/cdd/Bitstamp_BTCUSD_d.csv', verify=False).content
    response = response.decode('utf-8')
    response = "\n".join(response.split("\n")[1:])
    price_2 = pd.read_csv(io.StringIO(response))
    price_2['volatility'] = (1 - (price_2['Low'] / price_2['High'])) * 100
    price_2['date'] = pd.to_datetime(price_2['Date'])
    price_2.set_index('date', inplace=True)   
    price = price.join(price_2)
    price = price.loc[date_start:date_end]
    price['dif_high_close'] = ((price['High'] - price['Close']) / price['Close']) * 100
    return price

price = get_price_data()
 


# =============================================================================
# Time Series Chart
# =============================================================================
plt.figure(figsize=(15,20))
price_top = plt.subplot2grid((10,4), (0, 0), rowspan=2, colspan=4)
volatility = plt.subplot2grid((10,4), (2, 0), rowspan=1, colspan=4)
change_daily = plt.subplot2grid((10,4), (3, 0), rowspan=1, colspan=4)
tnx_vol_1 = plt.subplot2grid((10,4), (4,0), rowspan=1, colspan=4)
tnx_vol_2 = plt.subplot2grid((10,4), (5,0), rowspan=1, colspan=4)
tnx_vol_3 = plt.subplot2grid((10,4), (6,0), rowspan=1, colspan=4)
tnx_vol_4 = plt.subplot2grid((10,4), (7,0), rowspan=1, colspan=4)

price_top.plot(price.index, price['PriceUSD']) 
volatility.plot(price.index, price['volatility']) 
change_daily.plot(price.index, price['return']) 
change_daily.axhline(y=0, color='r', linestyle='-')

tnx_vol_1.bar(other_exchange.index, other_exchange['percent_marketcap']) 
tnx_vol_2.bar(exchange_other.index, exchange_other['percent_marketcap']) 
tnx_vol_3.bar(other_other.index, other_other['percent_marketcap']) 
tnx_vol_4.bar(exchange_exchange.index, exchange_exchange['percent_marketcap']) 
 
price_top.set_title('BTC transactions per category')
price_top.set_ylabel('Closing Price')
volatility.set_ylabel('Daily Volatility')
change_daily.set_ylabel('Daily Return')
tnx_vol_1.set_ylabel('unknown_exchange')
tnx_vol_2.set_ylabel('exchange_unknown')
tnx_vol_3.set_ylabel('unknown_unknown')
tnx_vol_4.set_ylabel('exchange_exchange')

change_daily.axes.get_xaxis().set_visible(False)
volatility.axes.get_xaxis().set_visible(False)
tnx_vol_1.axes.get_xaxis().set_visible(False)
tnx_vol_2.axes.get_xaxis().set_visible(False)
tnx_vol_3.axes.get_xaxis().set_visible(False)

plt.savefig('transaction_types_chart.png', transparent=True)  


# =============================================================================
# Analysis, Metrics
# =============================================================================
from datetime import timedelta


def analytics(df, price, mode='normal'):   
    return_m, return_0,return_1,return_2,vola_m,vola_0,vola_1,vola_2,diff_m,diff_0,diff_1,diff_2 = ([] for i in range(12))
    col_names = []
    index_name = [] 
    
    col_names.extend(['return-1', 'return_0', 'return_1', 'return_2', 'vola-1', 'vola_0', 'vola_1', 'vola_2', 'dif-1','diff_0', 'diff_1', 'diff_2'])
   
    if mode == 'average':
        index_name.append('AVERAGE')       
    else:        
        transaction_category = df['category'][0]
        index_name.append(str(transaction_category + '-whale'))
   
    for day in range(-1,3):      
        if mode == 'average':
            return_whale = price['return'].mean()
            vola_whale = price['volatility'].mean()
            diff_whale = price['dif_high_close'].mean()
          
        else:    
            df['date'] = df.index + timedelta(days=day)
            tmp = pd.merge(df, price, left_on='date', right_index=True, how='inner')
        
            whale = tmp[tmp['dollar'] != 0]        
            return_whale = whale['return'].mean()
            vola_whale = whale['volatility'].mean()
            diff_whale = whale['dif_high_close'].mean()
            #other = tmp[tmp['dollar'] == 0]
            #return_other = other['return'].mean()
            #vola_other = other['volatility'].mean()
            #diff_other = other['dif_high_close'].mean()
    
        if day == -1:
            return_m.extend([return_whale])
            vola_m.extend([vola_whale])
            diff_m.extend([diff_whale])
    
        if day == 0:
            return_0.extend([return_whale])
            vola_0.extend([vola_whale])
            diff_0.extend([diff_whale])
                    
        elif day == 1:
            return_1.extend([return_whale])
            vola_1.extend([vola_whale])
            diff_1.extend([diff_whale])
            
        elif day == 2:
            return_2.extend([return_whale])
            vola_2.extend([vola_whale])
            diff_2.extend([diff_whale])
    
    result = pd.DataFrame(list(zip(return_m,return_0, return_1, return_2,vola_m, vola_0, vola_1, vola_2, diff_m, diff_0, diff_1, diff_2)), index = index_name, columns = col_names) 
    return result
    

df = pd.DataFrame()
df = df.append(analytics(all_all, price, mode='average'))
df = df.append(analytics(exchange_exchange, price))
df = df.append(analytics(other_exchange, price))
df = df.append(analytics(exchange_other, price))
df = df.append(analytics(other_other, price))



# =============================================================================
# Other
# =============================================================================


'''
--Seaborn---

sns.set(style="whitegrid")
ax = sns.barplot(x="category", data=wallets, palette="Blues_d")
ax = sns.boxplot(x=tnx_valid["receiver_category"], y=tnx_valid["dollar"], showfliers=False) #boxplot
ax = sns.barplot(x="receiver_category", y='dollar', data=tnx_valid) #barplot
sns.distplot(tmp) #histogram
sns.countplot(x='receiver_category', data=tnx_valid) #countplot

#Scatterplot: https://seaborn.pydata.org/generated/seaborn.scatterplot.html
>>> ax = sns.scatterplot(x="total_bill", y="tip",
...                      hue="size", size="size",
...                      data=tips)
'''



