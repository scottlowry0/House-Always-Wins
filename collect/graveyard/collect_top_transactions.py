# -*- coding: utf-8 -*-
"""
Created on Fri Apr 24 16:05:51 2026

@author: Scott Lowry

A Script for collecting all transactions for users who participate in two
markets using the same wallet

Because of the way this script is calling the API, this script
can take a while depending on how many users and how active they are
"""

#Imports
import tomllib
import pandas as pd
from collect_utils import *
from plot_utils import *
import sys


#==========================================================================
#CONFIG
#==========================================================================
config_file = 'collect_config.toml'

with open(config_file,'rb') as fh:
    config = tomllib.load(fh)    
    activity_api = config['activity_api']
    markets = [config['market_underlying'], config['market_derivative']]
    
return_amount = 100
offset = 0
top_holders = pd.read_pickle('top_holders.pkl')

#==========================================================================
#CALLING API
#==========================================================================
#Separating out only those who participate in both markets
in_both = top_holders.loc[top_holders['In_Both'] == True].copy()
transactions_list = []

#Looping through both markets to find all transaction data for each user
#This loop can take a while depending on the number of users
#This loop only finds trades
for market in markets:
    for index in in_both.index: 
        params = {
                'user': in_both.loc[index, 'proxyWallet'],
                'market': market,
                'offset': 0,
                'limit': 100,
                'type': 'TRADE'
                }
        
        loop_number = 0
        
        #The api maxes at 3000 records, after which it only returns one records
        #This loop bypasses the limit by checking if the list had over
        #3000 records. If it did, those records are added to the list and
        #The timestamp of the last records is passed to the api on the next
        #turn through the loop. This repeats until all records for the user
        #has been collected.
        while True:
            user_data = call_api(activity_api, params, max_returns=3000)
            #End if the api returned nothing
            if not user_data:
                break
            transactions_list.extend(user_data)
            # If the max limit was reached, update parameters for the next batch
            if len(user_data) >= 3000:
                last_timestamp = user_data[-1]['timestamp']
                params['end'] = last_timestamp 
                params['offset'] = 0 
                loop_number += 1
                print(f'Loop number {loop_number}')
            #End if the api call gave less than 3000 records
            #means we collected everything
            else:
                break

#==========================================================================
#Formatting Data
#==========================================================================
#%%
#Creating a df of all the transactions
transactions_df = pd.DataFrame(transactions_list)

#Converting from Unix time and sorting data to chronological order
transactions_df['timestamp'] = pd.to_datetime(transactions_df['timestamp'], unit='s')
transactions_df = transactions_df.sort_values('timestamp').reset_index(drop=True)

#Making sells negative volume
transactions_df.loc[transactions_df['side']== 'SELL', 'size'] *= -1

#Finding dollar amount spent or gained per transaction
#Reinvert the size column so that sells gain money and buys lose money
transactions_df['transaction_cost'] = (transactions_df['size'] *-1) * transactions_df['price']

#Outputting to pickle file
pd.to_pickle(transactions_df, 'transactions.pkl')
