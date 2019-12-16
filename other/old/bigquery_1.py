# -*- coding: utf-8 -*-
"""
Spyder Editor

Dies ist eine temporäre Skriptdatei.

https://cloud.google.com/blog/products/data-analytics/introducing-six-new-cryptocurrencies-in-bigquery-public-datasets-and-how-to-analyze-them?_ga=2.43275237.-1394736426.1572708683
https://www.kaggle.com/wprice/bitcoin-mining-pool-classifier
https://bitcoindev.network/bitcoin-analytics-using-google-bigquery/

https://console.cloud.google.com/bigquery?project=crypto-257815&folder&organizationId&p=bigquery-public-data&d=crypto_bitcoin&t=transactions&page=table


Tutroial: https://towardsdatascience.com/https-medium-com-nocibambi-getting-started-with-bitcoin-data-on-kaggle-with-python-and-bigquery-d5266aa9f52b

"""
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=r"C:\Users\David\Dropbox\Code\Crypto-4c44e65fd97d.json"

from google.cloud import bigquery #pip install google-cloud-bigquery
client = bigquery.Client() #https://cloud.google.com/docs/authentication/getting-started

import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import numpy as np
import pandas as pd
import itertools


#Get all transactions over 50 BTC from today


# =============================================================================
# EXPLORATION
# =============================================================================

def get_atts(obj, filter=""):
    """Helper function wich prints the public attributes and methods of the given object.
    Can filter the results for simple term.
    - `obj`: the Python object of interest
    - `filter`, str: The filter term
    """
    return [a for a in dir(obj) if not a.startswith('_') and filter in a]


def estimate_gigabytes_scanned(query, bq_client):
    """A useful function to estimate query size. 
    Originally from here: https://www.kaggle.com/sohier/beyond-queries-exploring-the-bigquery-api/
    """
    # We initiate a `QueryJobConfig` object
    # API description: https://googleapis.dev/py/bigquery/latest/generated/google.cloud.bigquery.job.QueryJobConfig.html
    my_job_config = bigquery.job.QueryJobConfig()
    
    # We turn on 'dry run', by setting the `QueryJobConfig` object's `dry_run` attribute.
    # This means that we do not actually run the query, but estimate its running cost. 
    my_job_config.dry_run = True

    # We activate the job_config by passing the `QueryJobConfig` to the client's `query` method.
    my_job = bq_client.query(query, job_config=my_job_config)
    
    # The results comes as bytes which we convert into Gigabytes for better readability
    BYTES_PER_GB = 2**30
    estimate = my_job.total_bytes_processed / BYTES_PER_GB
    
    print(f"This query will process {estimate} GBs.")


get_atts(client)
get_atts(client, 'list')

project = 'bigquery-public-data'
dataset = 'crypto_bitcoin'

client.list_tables(f'{project}.{dataset}')

for ds in client.list_tables(f'{project}.{dataset}'):
    print(get_atts(ds))
    break

[ds.table_id for ds in client.list_tables(f'{project}.{dataset}')]

get_atts(client, 'table')

table = 'transactions'
trans_ref = client.get_table(f'{project}.{dataset}.{table}')
get_atts(trans_ref)

trans_ref.schema



blocks_firstrows_df = pd.DataFrame(
    [
        dict(row) for row
        in client.list_rows(trans_ref, max_results=5)
    ]
)
blocks_firstrows_df

blocks_firstrows_df['outputs'][0]






# =============================================================================
# QUERIES
# Let’s say we are interested in the frequency and value of bitcoin transactions during September 2017
# =============================================================================


query = """
    SELECT
        block_timestamp,
        input_count,
        input_value,
        output_count,
        output_value
    FROM `bigquery-public-data.crypto_bitcoin.transactions`
    WHERE
        EXTRACT(YEAR FROM block_timestamp) = 2017 AND
        EXTRACT(MONTH FROM block_timestamp) = 09 AND   
        EXTRACT(DAY FROM block_timestamp) = 01    
"""

estimate_gigabytes_scanned(query, client)


bytes_in_gigabytes = 2**30
safe_config = bigquery.QueryJobConfig(
    maximum_bytes_billed=200 * bytes_in_gigabytes
)
query_job = client.query(query, job_config=safe_config)


result = query_job.result()
df = result.to_dataframe()


mem_use = df.memory_usage(deep=True)
print(mem_use)
mem_use.sum() / bytes_in_gigabytes

df.to_csv('transactions_201709.csv', index=False)


# =============================================================================
# 
# =============================================================================

#Some Data for one day
query = """
    SELECT
        `hash`
        block_timestamp,
        input_count,
        input_value,
        output_count,
        output_value,
        is_coinbase,
        inputs,
        outputs   
    FROM `bigquery-public-data.crypto_bitcoin.transactions`
    WHERE
        EXTRACT(YEAR FROM block_timestamp) = 2018 AND
        EXTRACT(MONTH FROM block_timestamp) = 09 AND   
        EXTRACT(DAY FROM block_timestamp) = 01
"""


# Richest wallets
query = """
WITH double_entry_book AS (
   -- debits
   SELECT
    array_to_string(inputs.addresses, ",") as address
   , inputs.type
   , - inputs.value as value
   FROM `bigquery-public-data.crypto_bitcoin.inputs` as inputs
   UNION ALL
   -- credits
   SELECT
    array_to_string(outputs.addresses, ",") as address
   , outputs.type
   , outputs.value as value
   FROM `bigquery-public-data.crypto_bitcoin.outputs` as outputs
)
SELECT
   address
,   type   
,   sum(value) as balance
FROM double_entry_book
GROUP BY 1,2
ORDER BY balance DESC
LIMIT 1000
"""


#Transaction partners
query = """
SELECT
	txn_count,
	COUNT(txn_count) AS num_addresses
FROM
(
	SELECT 
	  ARRAY_TO_STRING(inputs.addresses, '') AS addresses,
	  COUNT(DISTINCT `hash`) AS txn_count
	FROM `bigquery-public-data.crypto_bitcoin.transactions` AS txns
	CROSS JOIN UNNEST(txns.inputs) AS inputs
	GROUP BY addresses
)
GROUP BY txn_count
ORDER BY txn_count ASC
"""



# all transactions
query = """
SELECT 
    `hash`
   , txns.block_timestamp
   , input_value
   , output_value
   , array_to_string(inputs.addresses, ",") as input_address
   , inputs.value as input_v
   , array_to_string(outputs.addresses, ",") as output_address
   , outputs.value as output_v
    
FROM `bigquery-public-data.crypto_bitcoin.transactions` AS txns
INNER JOIN `bigquery-public-data.crypto_bitcoin.inputs` AS inputs ON txns.hash = inputs.transaction_hash
INNER JOIN `bigquery-public-data.crypto_bitcoin.outputs` AS outputs ON txns.hash = outputs.transaction_hash

LIMIT 100
"""

#INNER JOIN txns.inputs AS inputs on txns.hash = inputs.transaction_hash
#INNER JOIN txns.outputs AS outputs on txns.hash = outputs.transaction_hash


query = """
SELECT 
    `hash`
   , txns.block_timestamp
   , input_value
   , output_value
  
FROM `bigquery-public-data.crypto_bitcoin.transactions` AS txns
WHERE input_value > 10000000000 AND
EXTRACT(YEAR FROM block_timestamp) >= 2019

ORDER BY input_value DESC
LIMIT 1000
"""


#get all transactios for one wallet
#### IMPORTANT: Timestamp: welches format, zeizonse

query = """
WITH all_transactions AS (
-- inputs
SELECT 
    transaction_hash
   , block_timestamp as timestamp
   , array_to_string(addresses, ",") as address
   , value
   , 'input' as type
FROM `bigquery-public-data.crypto_bitcoin.inputs`

UNION ALL

-- outputs
SELECT 
    transaction_hash
   , block_timestamp as timestamp
   , array_to_string(addresses, ",") as address
   , value
   , 'output' as type
FROM `bigquery-public-data.crypto_bitcoin.outputs`
)
SELECT
   transaction_hash
   , timestamp
   , address
   , value
   , type
FROM all_transactions
WHERE address = '35hK24tcLEWcgNA4JxpvbkNkoAcDGqQPsP'
"""


job_config = bigquery.QueryJobConfig()

bytes_in_gigabytes = 2**30
safe_config = bigquery.QueryJobConfig(
    maximum_bytes_billed=200 * bytes_in_gigabytes
)
query_job = client.query(query, job_config=safe_config)

estimate_gigabytes_scanned(query, client)

result = query_job.result()
df_address = result.to_dataframe()



df_4['input_value'] = pd.to_numeric(df_4['input_value'], errors='ignore')
df_4['BTC'] = df_4['input_value'] / 100000000
df = df_4

mem_use = df.memory_usage(deep=True)
print(mem_use)
mem_use.sum() / bytes_in_gigabytes

df_3.to_csv('data/partners.csv', index=False)



# =============================================================================
# Test
# =============================================================================

import importlib.util
spec = importlib.util.spec_from_file_location("module.name", "C:/Users/David/Dropbox/Code/config.py")
config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(config)
DB_CREDENTIALS = config.sqlalchemy_DATASTIG_CRYPTO  

from sqlalchemy import create_engine 
engine = create_engine(DB_CREDENTIALS)

df = pd.read_sql_query('''SELECT * FROM lyricmatcher WHERE id=4995''', engine) 







# =============================================================================
# Plotting
# =============================================================================


df['block_timestamp'] = pd.to_datetime(df['block_timestamp'] )
df.set_index('block_timestamp')['input_value'].plot(figsize=(18, 8))


(
    df
    .groupby(df['block_timestamp'].dt.weekday)
    ['input_value'].sum()
    .rename(index={i: day for i, day in enumerate(['Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa', 'Su'])})
    .plot(kind='bar', figsize=(18, 8))
)


df.groupby(df['block_timestamp'].dt.hour)['output_value'].sum().plot(kind='bar', figsize=(18, 8))



x = df[1:1000]

df.head()
