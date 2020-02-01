# -*- coding: utf-8 -*-
"""
Created on Thu Jan 23 19:34:11 2020

@author: David
"""

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

data = pd.read_csv("../data/features_trainingset_all_categories.csv")

data = pd.read_csv("../data/transactions_100BTC.csv", nrows=10000)

feature_importances.T.to_dict(orient='list')

data = pd.read_csv("../data/final_dataset/transactions_0.01_marketcap_2015-2020.csv")

d = data[['dollar', 'block_timestamp']]
max_v = data['dollar'].max()
min_v = data['dollar'].min()

fig, ax = plt.subplots()
graph = sns.countplot(x='category', data=wallets_known, color="black")


sns.catplot(x="category", y="mean_inputs", kind="box", data=data, showfliers=False, color="white")
plt.title('Mean number of inputs per category')
plt.savefig('features_mean_number_inputs.png', transparent=True)

sns.catplot(x="category", y="input_mean_tx_value_btc", kind="box", data=data, showfliers=False, color="white")
plt.title('Mean transaction value (btc) per category')
plt.savefig('features_mean_transaction_value.png', transparent=True)

sns.catplot(x="category", y="input_p_adr_tx_value_usd", kind="box", data=data, showfliers=False, color="white")
plt.title('Percent of input address value to transaction value per category')
plt.savefig('features_percent_address_inputs.png', transparent=True)


sns.catplot(x="category", y="input_mean_tx_value_btc", kind="box", data=data, showfliers=False)
sns.catplot(x="category", y="input_p_adr_tx_value_usd", kind="box", data=data, showfliers=False) #ratio of adr value to tx value
sns.catplot(x="category", y="outputs_p_adr_tx_value_usd", kind="box", data=data, showfliers=False)
sns.catplot(x="category", y="input_mode_tx_value_usd", kind="box", data=data, showfliers=False)
sns.catplot(x="category", y="std_tx_value_percent_marketcap", kind="box", data=data, showfliers=False)
sns.catplot(x="category", y="mean_tx_value_percent_marketcap", kind="box", data=data, showfliers=False)
sns.catplot(x="category", y="mean_value_percent_marketcap", kind="box", data=data, showfliers=False)
sns.catplot(x="category", y="input_max_tx_value_usd", kind="box", data=data, showfliers=False)
sns.catplot(x="category", y="outputs_mean_tx_value_btc", kind="box", data=data, showfliers=False)
sns.catplot(x="category", y="mean_outputs", kind="box", data=data, showfliers=False)


sns.catplot(x="category", y="n_tx", kind="box", data=data, showfliers=False)
sns.catplot(x="category", y="n_inputs", kind="box", data=data, showfliers=False)
sns.catplot(x="category", y="n_outputs", kind="box", data=data, showfliers=False)
sns.catplot(x="category", y="input_median_tx_value_usd", kind="box", data=data, showfliers=False)
sns.catplot(x="category", y="adr_dif_usd", kind="box", data=data, showfliers=False)



#######

features_unknown = pd.DataFrame()

for number in range(1,21):
    all_tnx = pd.read_csv("../data/address_unknown_chunk_" + str(number) + ".csv")    
    features_unknown = features_unknown.append(all_tnx)   

features_unknown.to_csv("address_unknown_batch_1.csv", index=False) 

###########


features_batch = pd.read_csv("../data/features_unknown_batch_1.csv")
features_batch_2 = pd.read_csv("../data/features_1.csv")

features = features_batch.append(features_batch_2)
features = features.drop_duplicates(subset='address')
features.to_csv("features_unknown.csv", index=False) 


features_batch = features_batch.drop_duplicates(subset='address')
features_batch.to_csv("features_unknown_batch_1.csv", index=False) 

##########


data.columns.values

data.n_tx.mean()


tmp = data.drop_duplicates(subset='address')
features_unknown.to_csv("features_unknown_batch_1.csv", index=False) 
scrape = pd.read_csv("../data/tmp/addresses_scrape_0.01_marketcap_2015-2020.csv")
known = pd.read_csv("../data/tmp/addresses_known_0.01_marketcap_2015-2020.csv")
unknown = pd.read_csv("../data/tmp/addresses_unknown_0.01_marketcap_2015-2020.csv")

tmp = pd.merge(features_batch, unknown, how='inner', on='address')

x = pd.merge(to_scrape, scrape, how='inner', on='address')

to_scrape = to_scrape.append(unknown)
to_scrape = to_scrape.drop_duplicates(subset='address', keep=False)


to_scrape.to_csv("SCRAPE.csv", index=False) 


x = known.drop_duplicates()


x = data.drop_duplicates(subset='address')

t1 = pd.read_csv("../data/address_known_chunk_1.csv")
t2 = pd.read_csv("../data/address_known_chunk_2.csv")
t3 = pd.read_csv("../data/address_known_chunk_3.csv")
t4 = pd.read_csv("../data/address_known_chunk_4.csv")

x = t1.append(t2)
x = x.append(t3)
x = x.append(t4)
x.to_csv("addresses_known.csv", index=False) 


known = pd.read_csv("../data/features_known.csv")

known_features = pd.merge(wallets, known, on='address')

tmp=known_features[['owner', 'category','address']]

x = tmp.groupby(['owner', 'category']).agg(['count'], as_index=False).reset_index()


known_features.to_csv("features_validation.csv", index=False) 


tmp = pd.merge(wallets, unknown, how='inner', on='address')

wallets.dtypes
data.dtypes


