# -*- coding: utf-8 -*-
"""
Created on Mon Nov 11 17:34:09 2019

@author: David
"""


# =============================================================================
# Compare transaction volume to price
# =============================================================================

timeframe = ['2018-01-01','2018-04-01']

'''Transactions BTC Value'''
tnx = df_large_transactions
tnx['Date'] = pd.to_datetime(tnx['block_timestamp'] )
#df['Date'] = pd.to_datetime(df['Date']).apply(lambda x: '{year}-{month}-{day}'.format(year=x.year, month=x.month, day=x.day))
#tnx['Date'] = pd.to_datetime(tnx.Date)
tnx["output_value"] = pd.to_numeric(df["output_value"] / BTC)
tnx.set_index('Date', inplace=True)
tnx = tnx[['output_value']].loc[timeframe[0]:timeframe[1]]
#df["output_value"].describe()


'''Load BTC Price'''
btc_price = pd.read_csv('BTC_USD.csv')

btc_price.set_index('Date', inplace=True)
btc_price_open = btc_price[['Open']].loc[timeframe[0]:timeframe[1]]
btc_price_close = btc_price[['Close']].loc[timeframe[0]:timeframe[1]]
btc_price_return = btc_price_close.pct_change(1)

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
txns_dollar = tnx.join(btc_price, how="inner")
txns_dollar['value'] = txns_dollar['output_value'] * txns_dollar['Close'] 
txns_dollar = txns_dollar[['value']]

res = txns_dollar[txns_dollar['value'] > 1000000]
#res.to_csv("transactions_1mio_2018.csv")
res = res.sort_values(by='Date') 
res = res.groupby('Date').sum()
#res = res.groupby('Date').max()
#res = res[res['output_value'] < 900000]
res.plot(figsize=(16, 8))

all_days = pd.date_range(timeframe[0], timeframe[1], freq='D')
res = res.reindex(all_days)


# =============================================================================
# Wallets Merge
# =============================================================================

labeled_transactions = df_large_transactions.merge(wallets, how='inner', on='address')
labeled_transactions["value"] = pd.to_numeric(labeled_transactions["value"] / BTC)
labeled_transactions['Date'] = pd.to_datetime(labeled_transactions['timestamp'] )
labeled_transactions.set_index('Date', inplace=True)
tmp = labeled_transactions[['value']]
tmp.plot(figsize=(16, 8))


df_large_transactions["value_btc"] = pd.to_numeric(df_large_transactions["value"] / BTC)
df_large_transactions['Date'] = pd.to_datetime(df_large_transactions['timestamp'] )
df_large_transactions.set_index('Date', inplace=True)
tmp2 = df_large_transactions[['value_btc']]
tmp2.plot(figsize=(16, 8))




# =============================================================================
# Plotting
# =============================================================================

'''Plot with 2 Plots'''
fig, ax = plt.subplots(figsize=(40, 8)) # Create the figure and axes object
res.plot(ax = ax) 
btc_price_return.plot(ax = ax, secondary_y = True) 
ax.axhline(secondary_y=0, color='r', linestyle='-', lw=2)



'''Plot with x Plots'''
fig, ax = plt.subplots(figsize=(30, 8))
ax3 = ax.twinx()
rspine = ax3.spines['right']
rspine.set_position(('axes', 1.15))
ax3.set_frame_on(True)
ax3.patch.set_visible(False)
fig.subplots_adjust(right=1)

ax4 = ax.twinx()
rspine = ax4.spines['right']
rspine.set_position(('axes', 1.15))
ax4.set_frame_on(True)
ax4.patch.set_visible(False)
fig.subplots_adjust(right=1)


btc_price_return.plot(ax=ax, style='r-')
#btc_price_close.plot(ax=ax, style='b-', secondary_y=True)
res.plot(ax=ax3, style='g-')
btc_price_open.plot(ax=ax4, style='y-')
ax.axhline(y=0, color='r', linestyle='-', lw=2)

# add legend 
ax3.legend([ax.get_lines()[0], ax.right_ax.get_lines()[0], ax3.get_lines()[0]],\
           ['A','B','C'], bbox_to_anchor=(1.5, 0.5))