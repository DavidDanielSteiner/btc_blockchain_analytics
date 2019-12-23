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
# Prepare Full Wallet Dataset for Classification
# =============================================================================
def get_all_tx_from_address_v1(list_addresses):
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
    
    return wallet_info
    
    
df = pd.read_csv("data/address_exchange_1.csv")
list_addresses = df['address'].to_list()
#list_addresses = ['1Evg7VMYi1wXGBPTY4j4xfMg4aMY3Fyr9R']
x = get_all_tx_from_address_v1(list_addresses)


# =============================================================================
# get all inputs
# =============================================================================
    
def get_all_tx_from_address_v2(list_addresses):
#get transaction overview from list of wallets

    query = """
    SELECT 
        array_to_string(i.addresses, ",") as address,
        i.transaction_hash,
        i.block_number,  
        i.block_timestamp,  
        
        i.value / 100000000 as value_btc ,
        t.`hash`,
        t.input_count,
        t.output_count,
        t.input_value / 100000000 as tx_value_btc , 
        'input' as type
        
    FROM `bigquery-public-data.crypto_bitcoin.inputs` as i     
    INNER JOIN `bigquery-public-data.crypto_bitcoin.transactions` AS t ON t.hash = i.transaction_hash    
    WHERE array_to_string(i.addresses, ',') in UNNEST(@address)
    LIMIT 25000
    
    UNION ALL
    
    SELECT 
        array_to_string(o.addresses, ",") as address,
        o.transaction_hash,
        o.block_number,  
        o.block_timestamp,  
        
        o.value / 100000000 as value_btc ,
        t.`hash`,
        t.input_count,
        t.output_count,
        t.input_value / 100000000 as tx_value_btc, 
        'output' as type
        
    FROM `bigquery-public-data.crypto_bitcoin.outputs` as o     
    INNER JOIN `bigquery-public-data.crypto_bitcoin.transactions` AS t ON t.hash = o.transaction_hash        
    WHERE array_to_string(o.addresses, ',') in UNNEST(@address)
    LIMIT 25000
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
    
    return wallet_info
    
df = pd.read_csv("data/address_exchange_1.csv")
list_addresses = df['address'].to_list()
all_tnx = get_all_tx_from_address_v2(list_addresses)

category = df[['address', 'class']]
df_features_2 = pd.merge(all_tnx,category,on='address',how='inner')

all_tnx.to_csv("testdata_5k_exchange.csv", index=False)
    
# =============================================================================
# get transactions with max transaction value
# =============================================================================

def get_tnx_max_value(btc):    
    btc_satoshi = 100000000 #btc in satoshi    
    satoshi_amount = btc * btc_satoshi
    
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
    WHERE outputs.value  >= @satoshis
        AND inputs.addresses IS NOT NULL
        AND outputs.addresses IS NOT NULL
    GROUP BY `hash`, block_timestamp, sender, receiver, value
    """
       
    query_params = [    
        bigquery.ScalarQueryParameter("satoshis", "INT64", satoshi_amount),
    ]
     
    estimate_gigabytes_scanned(query, client)
    
    job_config = bigquery.QueryJobConfig()
    job_config.query_parameters = query_params
    query_job = client.query(
            query,
            job_config=job_config,
    )
    result = query_job.result()
    
    large_transactions = result.to_dataframe()       
    large_transactions.to_csv("transactions_50BTC.csv", index=False)
    print("transactions saved to csv")
    return large_transactions


# =============================================================================
# Start
# =============================================================================

transactions = get_tnx_max_value(50)  
wallet_tnx = get_all_tnx_from_address(wallet_list)