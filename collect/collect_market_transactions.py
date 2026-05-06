# -*- coding: utf-8 -*-
"""
Created on Sun Apr 26 17:51:37 2026

A script that collects all users who traded in a market and then collects
All of their transactions in that market

In its current iteration, this script only works on smaller markets with
a limited number of users. Using this on a larger market will use all of
your RAM and probably will fail after a few hours.

@author: Scott Lowry
"""


#Imports
import tomllib
import pandas as pd
from collect_utils import *
import os


#==========================================================================
#CONFIG
#==========================================================================
script_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(script_dir)
config_file = os.path.join(repo_root, 'config.toml')

with open(config_file,'rb') as fh:
    config = tomllib.load(fh)    
    positions_api = config['positions_api']
    activity_api = config['activity_api']
    transaction_table_name = config['table_names']['transaction_table_name']
    market_table_name = config['table_names']['transaction_table_name']
    condition_id = config['market_to_collect']
    db_name = config['db_name']
    
db_path = os.path.join(repo_root, db_name)
return_amount = 100
offset = 0
conn = sqlite3.connect(db_path)
positions_df = pd.read_pickle('holders.pkl')

#==========================================================================
#CALLING API FOR TRANSACTION DATA
#==========================================================================
#%%

transactions_list = []
for index in positions_df.index: 
    params = {
            'user': positions_df.loc[index, 'proxyWallet'],
            'market': condition_id,
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
    print(f'Completed user {index} of {len(positions_df)}')


#==========================================================================
#FORMATTING DATA
#==========================================================================
#%%
transactions_df = pd.DataFrame(transactions_list)

#creates the transaction table if it does not yet exist
create_table(conn, 
             transactions_df, 
             transaction_table_name, 
             ['transactionHash'],
             [('conditionId', market_table_name)])

#upserts the data into the transactions table
upsert_data(conn,
            transactions_df, 
            transaction_table_name, 
            ['transactionHash'])

#Making a view of transactions for the market
view_name = transactions_df['slug'].iloc[0]
view_name = view_name.replace('-', '_')
create_market_transactions_view(conn, 
                                view_name,
                                transaction_table_name,
                                condition_id)

print(f'Script Complete. {len(transactions_df)} records added to database')
