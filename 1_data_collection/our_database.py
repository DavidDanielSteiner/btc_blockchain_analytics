# -*- coding: utf-8 -*-
"""
Created on Wed Dec  4 21:27:17 2019

@author: David
"""

import pandas as pd
import importlib.util
from sqlalchemy import create_engine 

spec = importlib.util.spec_from_file_location("module.name", "C:/Users/David/Dropbox/Code/config.py")
config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(config)
DB_CREDENTIALS = config.sqlalchemy_DATASTIG_CRYPTO  
engine = create_engine(DB_CREDENTIALS)

#wallets = pd.read_sql_query('''SELECT * FROM wallets_raw WHERE category ='Exchange' Limit 100''', engine) 
#wallets.to_csv("sample_exchanges_100k.csv", index=False)

def get_all_wallets_from_db():
    wallets = pd.read_sql_query('''SELECT * FROM wallets_raw''', engine) 
    return wallets

#wallets['category'].value_counts()
#wallets.loc[wallets['category'] != 'Exchange', 'category'] = 'Other'
#list_addresses =  wallets.address.values.tolist()
#list_addresses = ['35hK24tcLEWcgNA4JxpvbkNkoAcDGqQPsP']
