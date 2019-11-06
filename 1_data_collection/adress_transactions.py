# -*- coding: utf-8 -*-
"""
Created on Wed Nov  6 18:23:51 2019

@author: David
"""

import os
import pandas as pd
from google.cloud import bigquery #pip install google-cloud-bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=r"C:\Users\David\Dropbox\Code\Crypto-4c44e65fd97d.json" #https://cloud.google.com/docs/authentication/getting-started
client = bigquery.Client() 




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

df = pd.read_sql_query('''SELECT address FROM wallets_raw LIMIT 100''', engine) 

list_of_ids =  df.address.values.tolist()



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
    bigquery.ArrayQueryParameter("address", "STRING", list_of_ids),
]

job_config = bigquery.QueryJobConfig()
job_config.query_parameters = query_params
query_job = client.query(
    query,
    job_config=job_config,
)
result = query_job.result()
df_address = result.to_dataframe()
