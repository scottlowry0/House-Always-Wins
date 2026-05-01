# -*- coding: utf-8 -*-
"""
Created on Sun Apr 19 14:34:21 2026

@author: Scott Lowry

A script for collecting all of the transaction data from selected markets

this can be a lot of data, expect this script to run for some time
"""

#Imports
import tomllib
import pandas as pd
import requests
import sqlite3
import sys
from collect_utils import *
import os

#==========================================================================
#CONFIG
#==========================================================================
config_file = 'collect_config.toml'

with open(config_file,'rb') as fh:
    config = tomllib.load(fh)    
    api = config['transaction_api']
    db_name = config['db_name']
    tag_slug = config['tag_slug']
    event_table_name = config['table_names']['event_table_name']
    market_table_name = config['table_names']['market_table_name']
    tag_table_name = config['table_names']['tag_table_name']
    tag_bridge_table_name = config['table_names']['tag_bridge_table_name']
    transaction_table_name = config['table_names']['transaction_table_name']

return_amount = 100
offset = 0
conn = sqlite3.connect(db_name)
tag_slugs=['pop-culture']

#==========================================================================
#Calling API
#==========================================================================

#Building list of events to get
# #event_id_list = get_event_ids_by_tags(conn, 
#                                       tag_slugs, 
#                                       event_table_name,
#                                       tag_bridge_table_name,
#                                       tag_table_name)
event_id_list = ['21320']
    
params = {
    'limit': return_amount,
    'eventId': event_id_list,
    'offset': offset,
}

transaction_list = call_api(api, params, max_returns=3000)

#%%
#==========================================================================
#Adding the dataframe to the database
#==========================================================================

#Creating dataframe, creating table if needed, inserting data
transaction_df = pd.DataFrame(transaction_list)
create_table(conn, 
             transaction_df,
             transaction_table_name,
             ['transactionHash'])
upsert_data(conn, 
             transaction_df,
             transaction_table_name,
             ['transactionHash'])
print('script complete')

