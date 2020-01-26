# -*- coding: utf-8 -*-
"""
Created on Thu Jan 23 19:34:11 2020

@author: David
"""

import pandas as pd
import seaborn as sns

data = pd.read_csv("../data/features_trainingset_all_categories_cleaned.csv")

feature_importances.T.to_dict(orient='list')

'''
{'mean_inputs': [7721],
 'input_mean_tx_value_btc': [5965],
 'input_p_adr_tx_value_usd': [4600],
 'outputs_p_adr_tx_value_usd': [4269],
 'input_mode_tx_value_usd': [3751],
 'std_tx_value_percent_marketcap': [3576],
 'mean_tx_value_percent_marketcap': [3428],
 'mean_value_percent_marketcap': [3005],
 'input_max_tx_value_usd': [2707],
 'outputs_mean_tx_value_btc': [2685],
 'mean_outputs': [2657],
 'p_adr_dif_usd': [2472],
 'tx_total_value_usd': [2387],
 'adr_inputs_btc': [2180],
 'input_median_tx_value_usd': [1959],
 'input_mean_tx_value_usd': [1921],
 'outputs_mode_tx_value_usd': [1901],
 'mean_balance_btc': [1710],
 'adr_dif_usd': [1633],
 'outputs_max_tx_value_usd': [1612],
 'tx_per_day': [1506],
 'std_value_percent_marketcap': [1449],
 'median_balance_usd': [1398],
 'input_mode_value_usd': [1371],
 'outputs_mean_tx_value_usd': [1236],
 'outputs_median_tx_value_usd': [1179],
 'input_mean_value_btc': [1153],
 'adr_inputs_usd': [1099],
 'output_mode_value_usd': [1090],
 'std_balance_btc': [1015],
 'lifetime': [908],
 'max_balance_usd': [851],
 'adr_outputs_usd': [821],
 'std_inputs': [767],
 'input_max_value_usd': [738],
 'mean_balance_usd': [707],
 'output_mean_value_btc': [696],
 'input_mean_value_usd': [641],
 'input_median_value_usd': [640],
 'output_median_value_usd': [599],
 'output_max_value_usd': [577],
 'output_mean_value_usd': [423],
 'std_balance_usd': [410],
 'mode_balance_usd': [309],
 'adr_outputs_btc': [309],
 'std_outputs': [272],
 'input_std_tx_value_btc': [267],
 'outputs_std_tx_value_btc': [200],
 'input_std_tx_value_usd': [192],
 'p_inputs': [176],
 'outputs_std_tx_value_usd': [147],
 'input_std_value_btc': [126],
 'output_std_value_usd': [124],
 'output_std_value_btc': [86],
 'input_std_value_usd': [85],
 'p_1k': [69],
 'p_payback': [64],
 'n_inputs': [60],
 'n_outputs': [51],
 'n_tx': [41],
 'p_0k': [18],
 'p_10k': [11],
 'p_100k': [9],
 'p_1m': [0],
 'p_1b': [0],
 'p_10m': [0],
 'p_10b': [0],
 'p_100m': [0]}
'''

sns.catplot(x="category", y="mean_inputs", kind="box", data=data, showfliers=False)
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

wallets = pd.read_csv("../data/btc_wallets_new.csv")
known = pd.read_csv("../data/features_known.csv")

known_features = pd.merge(wallets, known, on='address')

tmp=known_features[['owner', 'category','address']]

x = tmp.groupby(['owner', 'category']).agg(['count'], as_index=False).reset_index()


known_features.to_csv("features_validation.csv", index=False) 


tmp = pd.merge(wallets, unknown, how='inner', on='address')

wallets.dtypes
data.dtypes


