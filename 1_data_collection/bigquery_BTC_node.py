# -*- coding: utf-8 -*-
"""
Created on Wed Nov  6 18:23:51 2019

@author: David

https://console.cloud.google.com/bigquery?project=crypto-257815&folder&organizationId&p=bigquery-public-data&d=crypto_bitcoin&t=transactions&page=table
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery #pip install google-cloud-bigquery
import matplotlib.pyplot as plt 
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=r"C:\Users\David\Dropbox\Code\Crypto-4c44e65fd97d.json" #https://cloud.google.com/docs/authentication/getting-started
client = bigquery.Client() 



# =============================================================================
# Helpers
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
    my_job_config = bigquery.job.QueryJobConfig()
    my_job_config.dry_run = True
    my_job = bq_client.query(query, job_config=my_job_config)
    BYTES_PER_GB = 2**30
    estimate = my_job.total_bytes_processed / BYTES_PER_GB    
    print(f"This query will process {estimate} GBs.")


# =============================================================================
# Prepare Full Wallet Dataset for Binary Classification
# =============================================================================
def get_all_tnx(list_addresses):
#get transaction overview from list of wallets

    query = """
    WITH all_transactions AS (
    -- inputs
    SELECT 
        transaction_hash
       , block_timestamp as timestamp
       , array_to_string(addresses, ",") as address
       , value
       , 'sent' as type
    FROM `bigquery-public-data.crypto_bitcoin.inputs`
    
    UNION ALL
    
    -- outputs
    SELECT 
        transaction_hash
       , block_timestamp as timestamp
       , array_to_string(addresses, ",") as address
       , value
       , 'received' as type
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
    wallet_info = result.to_dataframe()
    wallet_info.to_csv("tnx_wallets.csv")


# =============================================================================
# get transactions with max transaction value
# =============================================================================

def get_tnx_max_value(btc):    
    btc_satoshi = 100000000 #btc in satoshi    
    
    query = """
    SELECT
        `hash`,
        block_timestamp,
        array_to_string(inputs.addresses, ",") as sender,
        array_to_string(outputs.addresses, ",") as receiver,
        output_value / 100000000 as value
    FROM `bigquery-public-data.crypto_bitcoin.transactions`
        JOIN UNNEST (inputs) AS inputs
        JOIN UNNEST (outputs) AS outputs
    WHERE outputs.value  >= 50000000000
        AND inputs.addresses IS NOT NULL
        AND outputs.addresses IS NOT NULL
    GROUP BY `hash`, block_timestamp, sender, receiver, value
    """
    
    job_config = bigquery.QueryJobConfig()
    query_job = client.query(query)
    result = query_job.result()
    large_transactions = result.to_dataframe()
    large_transactions.to_csv("transactions_500BTC_2.csv", index=False)
    
    
    
    satoshi_amount = 500 * btc_satoshi
    query_params = [    
        bigquery.ScalarQueryParameter("satoshis", "INT64", satoshi_amount),
    ]
    
    
    estimate_gigabytes_scanned(query, client)
    
    job_config = bigquery.QueryJobConfig()
    job_config.query_parameters = query_params
    query_job = client.query(query)
    result = query_job.result()
    
    large_transactions = result.to_dataframe()
    large_transactions.head()
    
    large_transactions.to_csv("transactions_50BTC.csv", index=False)





'''
wallets = pd.read_sql_query('''SELECT * FROM wallets_raw WHERE category ='Exchange' Limit 100000''', engine) 
wallets.to_csv("sample_exchanges_100k.csv", index=False)
#wallets.to_csv("sample_other_100k.csv", index=False)

wallets['category'].value_counts()
wallets.loc[wallets['category'] != 'Exchange', 'category'] = 'Other'
list_addresses =  wallets.address.values.tolist()
#list_addresses = ['35hK24tcLEWcgNA4JxpvbkNkoAcDGqQPsP']

'''