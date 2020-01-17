# -*- coding: utf-8 -*-
"""
Created on Tue Dec 10 20:00:53 2019

@author: David
"""

import pandas as pd
tnx = pd.read_csv("data/transactions_valid.csv", index_col=False)

tmp1 = tnx.groupby(['receiver']).agg({'hash':['count', 'nunique', lambda x: x.count() / x.nunique()],
                         'receiver_name':'first'})

#unique_tnx = tnx.drop_duplicates(subset='hash', keep='last')
tmp2 = tnx_valid.groupby(['receiver']).agg({'hash':'count',
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