# -*- coding: utf-8 -*-
"""
Created on Thu Dec  5 12:19:36 2019

@author: David
"""

import pandas as pd

wallets = pd.DataFrame()

df = pd.read_csv("data/wallets_walletexplorer.csv", index_col=False)
wallets = wallets.append(df)

df = pd.read_csv("data/addresses/Mining_full_detailed.csv", index_col=False)
df.rename(columns = {"mining": "owner", "hashAdd":"address"}, inplace = True) 
df['category'] = 'Pools'
df = df[['address', 'owner', 'category']]
wallets = wallets.append(df)

df = pd.read_csv("data/addresses/Exchanges_full_detailed.csv", index_col=False)
df.rename(columns = {"exchange": "owner", "hashAdd":"address"}, inplace = True) 
df['category'] = 'Exchange'
df = df[['address', 'owner', 'category']]
wallets = wallets.append(df)

df = pd.read_csv("data/addresses/Gambling_full_detailed.csv", index_col=False)
df.rename(columns = {"gambling": "owner", "hashAdd":"address"}, inplace = True) 
df['category'] = 'Gambling'
df = df[['address', 'owner', 'category']]
wallets = wallets.append(df)

df = pd.read_csv("data/addresses/Historic_full_detailed.csv", index_col=False)
df.rename(columns = {"historic": "owner", "hashAdd":"address"}, inplace = True) 
df['category'] = 'Historic'
df = df[['address', 'owner', 'category']]
wallets = wallets.append(df)

df = pd.read_csv("data/addresses/Services_full_detailed.csv", index_col=False)
df.rename(columns = {"service": "owner", "hashAdd":"address"}, inplace = True) 
df['category'] = 'Services'
df = df[['address', 'owner', 'category']]
wallets = wallets.append(df)

wallets = wallets.drop_duplicates(subset='address', keep='last')
wallets = wallets[wallets['category'] != 'Historic']

# =============================================================================
# Recategorize Historic 
# =============================================================================
tmp = wallets[wallets['category'] == 'Historic']
tmp = tmp.drop_duplicates(subset='owner', keep='last')
list_o = tmp['owner'].tolist()

wallets.loc[wallets.owner == 'MiddleEarthMarketplace', 'category'] = 'Service'
wallets.loc[wallets.owner == 'AbraxasMarket', 'category'] = 'Service'
wallets.loc[wallets.owner == 'PandoraOpenMarket', 'category'] = 'Service'
wallets.loc[wallets.owner == 'AgoraMarket', 'category'] = 'Service'
wallets.loc[wallets.owner == 'SilkRoad2Market', 'category'] = 'Service'
wallets.loc[wallets.owner == 'BabylonMarket', 'category'] = 'Service'
wallets.loc[wallets.owner == 'SilkRoadMarketplace', 'category'] = 'Service'
wallets.loc[wallets.owner == 'CannabisRoadMarket', 'category'] = 'Service'
wallets.loc[wallets.owner == 'BlackBankMarket', 'category'] = 'Service'
wallets.loc[wallets.owner == 'EvolutionMarket', 'category'] = 'Service'
wallets.loc[wallets.owner == 'SheepMarketplace', 'category'] = 'Service'
wallets.loc[wallets.owner == 'BlueSkyMarketplace', 'category'] = 'Service'

wallets.loc[wallets.owner == 'DiceBitco', 'category'] = 'Gambling'
wallets.loc[wallets.owner == 'Just-Dice.com', 'category'] = 'Gambling'
wallets.loc[wallets.owner == 'CryptoBounty.com', 'category'] = 'Gambling'
wallets.loc[wallets.owner == 'Betcoins.net', 'category'] = 'Gambling'
wallets.loc[wallets.owner == 'BtcDice.com', 'category'] = 'Gambling'
wallets.loc[wallets.owner == 'BetsOfBitco.in', 'category'] = 'Gambling'
wallets.loc[wallets.owner == 'PinballCoin.com', 'category'] = 'Gambling'
wallets.loc[wallets.owner == 'Playt.in', 'category'] = 'Gambling'
wallets.loc[wallets.owner == 'EveryDice.com', 'category'] = 'Gambling'
wallets.loc[wallets.owner == 'BetcoinDice.tm', 'category'] = 'Gambling'
wallets.loc[wallets.owner == 'SuzukiDice.com', 'category'] = 'Gambling'
wallets.loc[wallets.owner == 'Bitcoin-Roulette.com', 'category'] = 'Gambling'
wallets.loc[wallets.owner == 'DiceOnCrack.com', 'category'] = 'Gambling'
wallets.loc[wallets.owner == 'Ice-Dice.com', 'category'] = 'Gambling'
wallets.loc[wallets.owner == 'DaDice.com', 'category'] = 'Gambling'

wallets.loc[wallets.owner == 'Comkort.com', 'category'] = 'Exchange'
wallets.loc[wallets.owner == 'Bitcash.cz', 'category'] = 'Exchange'
wallets.loc[wallets.owner == 'BitcoinWeBank.com', 'category'] = 'Exchange'
wallets.loc[wallets.owner == 'BtcExchange.ro', 'category'] = 'Exchange'
wallets.loc[wallets.owner == 'MasterXchange.com', 'category'] = 'Exchange'
wallets.loc[wallets.owner == 'Crypto-Trade.com', 'category'] = 'Exchange'

wallets.loc[wallets.owner == 'ASICMiner', 'category'] = 'Pools'
wallets.loc[wallets.owner == 'MinersCenter.com', 'category'] = 'Pools'
wallets.loc[wallets.owner == 'CryptcoMiner.com', 'category'] = 'Pools'
wallets.loc[wallets.owner == 'Cryptomine.io', 'category'] = 'Pools'
wallets.loc[wallets.owner == '50BTC.com', 'category'] = 'Pools'
wallets.loc[wallets.owner == 'Polmine.pl', 'category'] = 'Pools'


# =============================================================================
# Export data
# =============================================================================

wallets.to_csv("data/wallets.csv", index = False)
wallets = pd.read_csv("data/wallets.csv", index_col=False)
from sqlalchemy import create_engine 
import importlib.util

#DB connection
spec = importlib.util.spec_from_file_location("module.name", "C:/Users/David/Dropbox/Code/config.py")
config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(config)
DB_CREDENTIALS = config.sqlalchemy_DATASTIG_CRYPTO  
engine = create_engine(DB_CREDENTIALS)

wallets.to_sql("wallets", engine, index=False, if_exists='append') 

