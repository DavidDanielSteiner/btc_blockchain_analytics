# -*- coding: utf-8 -*-
"""
Created on Wed Nov  6 18:23:51 2019

@author: David
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery #pip install google-cloud-bigquery
import matplotlib.pyplot as plt # Impot the relevant module
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=r"C:\Users\David\Dropbox\Code\Crypto-4c44e65fd97d.json" #https://cloud.google.com/docs/authentication/getting-started
client = bigquery.Client() 

BTC = 100000000 #btc in satoshi

# =============================================================================
# SQL
# =============================================================================

import importlib.util
spec = importlib.util.spec_from_file_location("module.name", "C:/Users/David/Dropbox/Code/config.py")
config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(config)
DB_CREDENTIALS = config.sqlalchemy_DATASTIG_CRYPTO  

from sqlalchemy import create_engine 
engine = create_engine(DB_CREDENTIALS)

df = pd.read_sql_query('''SELECT address FROM wallets_raw LIMIT 10000''', engine) 

list_addresses =  df.address.values.tolist()


#get transaction overview from list of wallets
query = """
WITH all_transactions AS (
-- inputs
SELECT 
    transaction_hash
   , block_timestamp as timestamp
   , array_to_string(addresses, ",") as address
   , value
   , 'received' as type
FROM `bigquery-public-data.crypto_bitcoin.inputs`

UNION ALL

-- outputs
SELECT 
    transaction_hash
   , block_timestamp as timestamp
   , array_to_string(addresses, ",") as address
   , value
   , 'sent' as type
FROM `bigquery-public-data.crypto_bitcoin.outputs`
)

SELECT
   address
   , type
   , sum(value) as sum
   , avg(value) as avg
   , min(value) as min
   , max(value) as max
   , count(transaction_hash) as number_transactions
   , min(timestamp) as first_transaction
   , max(timestamp) as last_transaction
FROM all_transactions
WHERE address in UNNEST(@address)
GROUP BY type, address
"""

query_params = [    
    bigquery.ArrayQueryParameter("address", "STRING", list_addresses),
]

job_config = bigquery.QueryJobConfig()
job_config.query_parameters = query_params
query_job = client.query(
    query,
    job_config=job_config,
)
result = query_job.result()
df_address = result.to_dataframe()

#df_address.to_csv("df_address_10000.csv")


#get wallets with max transaction value
query = """
SELECT 
     block_timestamp
   , output_value
  
FROM `bigquery-public-data.crypto_bitcoin.transactions` AS txns
WHERE output_value > 10000000000 AND
EXTRACT(YEAR FROM block_timestamp) = 2018

ORDER BY output_value DESC
"""


job_config = bigquery.QueryJobConfig()
job_config.query_parameters = query_params
query_job = client.query(query)
result = query_job.result()
df_max_transactions = result.to_dataframe()





# =============================================================================
# 
# =============================================================================

def compute_Hourly_Returns(data):
    data["Average_hourly_price"]=(data["close"]+data["open"])/2
    data["Hourly_returns"]=data["Average_hourly_price"].divide(data["Average_hourly_price"].shift())-1
    data= data.iloc[1:] 
    return data

#Set the start and the end date:
start_unix=time.mktime(datetime.datetime.strptime("30/03/2017", "%d/%m/%Y").timetuple())
end_unix=time.mktime(datetime.datetime.strptime("20/07/2019", "%d/%m/%Y").timetuple())

#Call the functions to retrieve the cryptocurrency data:
bitcoin=compute_Hourly_Returns(get_df_spec('histohour',"BTC", start_unix, end_unix))



# =============================================================================
# Compare transaction volume to price
# =============================================================================

timeframe = ['2018-01-01','2018-04-01']


'''Transactions BTC Value'''
df = df_max_transactions
df['Date'] = pd.to_datetime(df['block_timestamp'] )
#df['Date'] = pd.to_datetime(df['Date']).apply(lambda x: '{year}-{month}-{day}'.format(year=x.year, month=x.month, day=x.day))
df['Date'] = pd.to_datetime(df.Date)
df["output_value"] = pd.to_numeric(df["output_value"] / BTC)
df.set_index('Date', inplace=True)
df = df[['output_value']].loc[timeframe[0]:timeframe[1]]
#df["output_value"].describe()


'''Load BTC Price'''
btc_price = pd.read_csv('BTC_USD.csv')
btc_price['Date'] = pd.to_datetime(btc_price.Date)
btc_price.set_index('Date', inplace=True)
btc_price_open = btc_price[['Open']].loc[timeframe[0]:timeframe[1]]
btc_price_close = btc_price[['Close']].loc[timeframe[0]:timeframe[1]]
btc_price_return = btc_price_open.pct_change(1)

#Plottting
fig, ax = plt.subplots(figsize=(16, 8)) 
btc_price_return.plot(ax = ax) 
ax.axhline(y=0, color='r', linestyle='-', lw=2)

#Plot
fig, ax = plt.subplots(figsize=(40, 8)) 
btc_price_open.plot(ax = ax) 
btc_price_close.plot(ax = ax) 
btc_price_return.plot(ax = ax, secondary_y = True)


'''Transactions Dollar Value'''
#Dollar value
txns_in_dollar = df.join(btc_price, how="inner")
txns_in_dollar['value'] = txns_in_dollar['output_value'] * txns_in_dollar['Close'] 
txns_in_dollar = txns_in_dollar[['value']]

res = txns_in_dollar[txns_in_dollar['value'] > 1000000]
#res.to_csv("transactions_1mio_2018.csv")
res = res.sort_values(by='Date') 
#res = res.groupby('Date').sum()
res = res.groupby('Date').max()
#res = res[res['output_value'] < 900000]
res.plot(figsize=(16, 8))

all_days = pd.date_range(timeframe[0], timeframe[1], freq='D')
res = res.reindex(all_days)


# =============================================================================
# Plotting
# =============================================================================

fig, ax = plt.subplots(figsize=(40, 8)) # Create the figure and axes object
res.plot(ax = ax) 
btc_price_return.plot(ax = ax, secondary_y = True) 
ax.axhline(secondary_y=0, color='r', linestyle='-', lw=2)




import matplotlib.pyplot as plt
import numpy as np
from pandas import DataFrame
#df = DataFrame(np.random.randn(5, 3), columns=['A', 'B', 'C'])

fig, ax = plt.subplots(figsize=(30, 8))
ax3 = ax.twinx()
rspine = ax3.spines['right']
rspine.set_position(('axes', 1.15))
ax3.set_frame_on(True)
ax3.patch.set_visible(False)
fig.subplots_adjust(right=1)

res.plot(ax=ax, style='b-')
# same ax as above since it's automatically added on the right
btc_price_close.plot(ax=ax, style='r-', secondary_y=True)
btc_price_return.plot(ax=ax3, style='g-')

# add legend --> take advantage of pandas providing us access
# to the line associated with the right part of the axis
ax3.legend([ax.get_lines()[0], ax.right_ax.get_lines()[0], ax3.get_lines()[0]],\
           ['A','B','C'], bbox_to_anchor=(1.5, 0.5))