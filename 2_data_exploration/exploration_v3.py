# -*- coding: utf-8 -*-
"""
Created on Mon Nov 18 20:57:26 2019

@author: David
"""

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# =============================================================================
# LABELS
# =============================================================================

wallets = pd.read_csv("data/wallets_walletexplorer.csv", index_col=False)
wallets.head()

tips = sns.load_dataset("tips")

sns.set(style="whitegrid")
ax = sns.barplot(x="category", data=wallets, palette="Blues_d")

g = sns.catplot(x="sex", y="total_bill",
                hue="smoker", col="time",
                data=tips, kind="bar",
                height=4, aspect=.7);

                

# =============================================================================
#                 
# =============================================================================
test = tnx[tnx['hash'] == 'fdb44638f7fb8184581e3847a79c0906f4afe85ec1dc68b406b9a011e6e7d49b']
           
                
tnx = pd.read_csv("data/transactions_10MIO.csv", index_col=False)
tnx["btc"] = tnx["btc"].astype(int)
tnx["dollar"] = tnx["dollar"].astype(int)
tnx = tnx.fillna('unknown')     
tnx.sort_values(by=['date'], inplace=True, ascending=True)
#tnx_grouped = pd.read_csv("data/transactions_unique_10MIO.csv", index_col=False)

#How many senders and receivers per transaction
tmp = tnx.groupby(['hash']).nunique()
tmp = tmp[tmp['sender'] == 1]
sns.boxplot(x=tmp["sender"])

#transactions with single sender /self transactions
tmp = tnx[tnx.groupby('hash')['sender'].transform('size') == 1]
self_transactions = tmp[tmp['sender'] == tmp['receiver']] #43484 self transactions
sns.countplot(x='receiver_category', data=self_transactions) 
sns.countplot(x='receiver_name', data=self_transactions) 
self_transactions['receiver_name'].value_counts(normalize=True).plot(kind='bar')

ax = sns.boxplot(x=self_transactions["dollar"], y=self_transactions["receiver_name"], showfliers=False) 

plt.figure(figsize = (20,15))
plt.xticks(rotation=45)
ax = sns.countplot(x='receiver_name', data=self_transactions, order = self_transactions.receiver_name.value_counts().index) 



#grouped transactions without self transactions
tnx_grouped_all = tnx.groupby(['hash'], as_index=False).first()
tnx_grouped = pd.concat([tnx_grouped_all, self_transactions]).drop_duplicates(keep=False)

ax = sns.boxplot(x=tnx_grouped["receiver_category"], y=tnx_grouped["dollar"], showfliers=False) 
sns.countplot(x='receiver_category', data=tnx_grouped) 

plt.figure(figsize = (20,15))
plt.xticks(rotation=45)
ax = sns.countplot(x='receiver_name', data=tnx_grouped, order = tnx_grouped.receiver_name.value_counts().index) 

ax = sns.boxplot(x=tnx_grouped["receiver_category"], y=tnx_grouped["percent_marketcap"])
ax = sns.boxplot(x=tnx_grouped["receiver_category"], y=tnx_grouped["percent_marketcap"], showfliers=False)
plt.figure(figsize = (15,15))

ranks = tnx_grouped.groupby("receiver_name")["dollar"].median().fillna(0).sort_values()[::-1].index

# Plot the orbital period with horizontal boxes
ax = sns.boxplot(x="distance", y="method", data=planets,
                 whis=np.inf, color="c",  order = ranks)

plt.figure(figsize = (15,15))
ax = sns.boxplot(x=tnx_grouped["dollar"], y=tnx_grouped["receiver_name"], order = ranks) 
ax = sns.boxplot(x=tnx_grouped["btc"], y=tnx_grouped["receiver_name"], showfliers=False) 



#Scatterplot (Date, Dollar, BTC)
plt.figure(figsize = (30,30))
cmap = sns.cubehelix_palette(dark=.3, light=.8, as_cmap=True)
ax = sns.scatterplot(x="date", y="dollar",
                      hue="receiver_category", size="btc",
                      palette="Set2",
                      data=tnx_grouped)


'''
ax = sns.boxplot(x=tnx_grouped["receiver_category"], y=tnx_grouped["dollar"], showfliers=False) #boxplot
#ax = sns.barplot(x="receiver_category", y='dollar', data=tnx_grouped) #barplot
sns.distplot(tmp) #histogram
sns.countplot(x='receiver_category', data=tnx_grouped) #countplot

#Scatterplot: https://seaborn.pydata.org/generated/seaborn.scatterplot.html
>>> ax = sns.scatterplot(x="total_bill", y="tip",
...                      hue="size", size="size",
...                      data=tips)


#self_transactions = pd.merge(tmp2, tnx, left_index=True, right_on='hash')
#self_transactions = tnx.drop_duplicates('hash', keep='last')
'''

