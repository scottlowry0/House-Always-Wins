# -*- coding: utf-8 -*-
"""
Created on Thu Apr 30 22:15:17 2026

@author: Scott Lowry
"""
#Imports
import os
import tomllib
import sqlite3
import pandas as pd

#==========================================================================
#CONFIG
#==========================================================================
script_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(script_dir)
config_file = os.path.join(repo_root, 'config.toml')

with open(config_file,'rb') as fh:
    config = tomllib.load(fh)
    db_name = config['db_name']
    underlying_view_name = config['underlying_view_name']
    derivative_view_name = config['derivative_view_name']
    holders_pickle_name = config['holders_pickle_name']

db_path = os.path.join(repo_root, db_name)    
conn = sqlite3.connect(db_path)

#==========================================================================
#DATA CLEANING
#==========================================================================
#%%
#Importing data from database
query = f"SELECT * FROM {underlying_view_name}"
underlying = pd.read_sql_query(query, conn)
query = f"SELECT * FROM {derivative_view_name}"
derivative = pd.read_sql(query, conn)

#Importing users pkl
holders = pd.read_pickle(os.path.join(repo_root, 'holders.pkl'))

#Checking for users who participated in both markets
holders['in_both'] = holders['proxyWallet'].isin(underlying['proxyWallet'])

#Finding splits and merges and making them count as both yes and no
splits_and_merges = derivative[derivative['type'].isin(['SPLIT', 'MERGE'])].copy()
yes_shares = splits_and_merges.copy()
yes_shares['outcome'] = 'Yes'
no_shares = splits_and_merges.copy()
no_shares['outcome'] = 'No'
print(splits_and_merges['size'].sum())

#Removing the original splits and merges from the df to not double count
#then readding as a yes and a no
derivative_trades_only = derivative[~derivative['type'].isin(['SPLIT', 'MERGE'])]
derivative = pd.concat([derivative_trades_only, yes_shares, no_shares], ignore_index=True)

#Removing redeems
derivative = derivative[derivative['type'] != 'REDEEM']


#Making sells and merges negative volume
derivative.loc[derivative['side'] == 'SELL', 'size'] = derivative['size'] * -1
derivative.loc[derivative['type'] == 'MERGE', 'size'] = derivative['size'] * -1


#==========================================================================
#ANALYSIS OF MARKET VOLUME
#==========================================================================
#%%

#Calculating number of oustanding shares and number of wallets in both markets
volume = derivative['size'].sum()
total_both = holders['in_both'].sum()
percent_both = holders["in_both"].mean() * 100
total_wallets = len(holders)

#Findings the sum of volumes for each trader
volumes_df = derivative.groupby(['proxyWallet', 'name', 'pseudonym'])['size'].sum().reset_index()
volumes_df['size'] = volumes_df['size'].round(4)
volumes_df['size'] = volumes_df['size'].clip(lower=0)

#Separating df by outcome
derivative_yes = derivative.loc[derivative['outcome'] == 'Yes']
derivative_no = derivative.loc[derivative['outcome'] == 'No']

#finding the volumes ownder by top users
yes_volumes = derivative_yes.groupby(['proxyWallet', 'name', 'pseudonym'])['size'].sum().reset_index()
no_volumes = derivative_no.groupby(['proxyWallet', 'name', 'pseudonym'])['size'].sum().reset_index()


#Summing Volume Amounts   
no_total_volume = no_volumes['size'].sum()
yes_total_volume = yes_volumes['size'].sum()
volume_check = yes_total_volume + no_total_volume
 
print(f'Outstanding shares value: {volume}')  
print(f'Number of wallets in the market: {total_wallets}')
print(f'number of wallets participating in both markets {total_both}')
print(f'Percent of wallets participating in undelying market: {percent_both}')
print(f'Total volume for yes: {yes_total_volume}')
print(f'Total volume for no: {no_total_volume}')
print(f'volume check: {volume_check}')

#==========================================================================
#ANALYSIS OF USERS IN BOTH MARKETS
#==========================================================================

