# -*- coding: utf-8 -*-
"""
Created on Mon Dec 16 14:49:53 2019

@author: David
"""



df = df.sort_values('block_number')    
df = df.reset_index(drop=True)

df["value_btc"] = pd.to_numeric(df["value_btc"])
df["tx_value_btc"] = pd.to_numeric(df["tx_value_btc"])
df['balance_btc'] = 0.0

#Calculate Balance
for index, row in df.iterrows():
    if index == 0:
        balance = 0
    else:
        balance = df.iloc[[index - 1]]['balance_btc']
        
    if row['type'] == 'output':
        balance += row['value_btc']
    else:
        balance -= row['value_btc']
    df.at[index,'balance_btc'] = balance
 
#Add Dollar Price 
btc_price_data = pd.read_csv("data/btc_price_data.csv")
btc_price_data = btc_price_data[['date', 'CapMrktCurUSD','PriceUSD']]
btc_price_data['date'] = pd.to_datetime(btc_price_data['date']).apply(lambda x: '{year}-{month}-{day}'.format(year=x.year, month=x.month, day=x.day))  

df['date'] = pd.to_datetime(df['block_timestamp']).apply(lambda x: '{year}-{month}-{day}'.format(year=x.year, month=x.month, day=x.day))
df = pd.merge(df, btc_price_data, on='date', how='inner')

df['value_dollar'] = df['value_btc'] * df['PriceUSD']
df['value_percent_marketcap'] = (df['value_dollar'] / df['CapMrktCurUSD']) *100
df['balance_dollar'] = df['balance_btc'] * df['PriceUSD']

df = df.drop(['transaction_hash', 'CapMrktCurUSD', 'PriceUSD'], axis = 1) 


#feature engineering
     

  
df.dtypes    
df.columns.values
df.iloc[[1]]
