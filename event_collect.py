# -*- coding: utf-8 -*-
"""
Created on Sun Apr 12 13:09:01 2026

@author: Scott Lowry

A first draft script for using the polymarket API to build a table of events

*********************************************************
THIS SCRIPT WILL DROP TABLES FROM THE DATABASE BEING USED
*********************************************************
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
config_file = 'config.toml'

with open(config_file,'rb') as fh:
    config = tomllib.load(fh)    
    api = config['api']
    db_name = config['db_name']
    tag_slug = config['tag_slug']
    include_chat = config['include_chat']
    event_table_name = config['table_names']['event_table_name']
    market_table_name = config['table_names']['market_table_name']
    tag_table_name = config['table_names']['tag_table_name']
    active = config['market_status']['active']
    closed = config['market_status']['closed']


conn = sqlite3.connect(db_name)

#offset should always start as 0
#increment amount is the number of results that each call will return
#max returns is the max number of events to return
offset = 0
return_amount = 5
max_returns = 1500
events_list = []

#%%
#==========================================================================
#Calling API
#==========================================================================

#Loop for calling the api to collect the market data
#Depending on the tags specificed, this can take a while
print('Begining script')
while offset <= max_returns:
   
    params = {
        'limit': return_amount,
        'tag_slug': tag_slug,
        'include_chat': include_chat,
        'offset': offset,
        'active': active,
        'closed': closed
    }
    
    try:
        response = requests.get(api, params=params)
        retry = 0
    
    except Exception as e:
            print('Exception',e)
            print(response.status_code)
            print(response.text)
            if retry < 3:
                retry += 1
                print('\nRetry',retry)
            else:
                print('\nMaximum retries exceeded')
                sys.exit()
                
    
    #Formatting a succesful call
    row_list = response.json()
    
    #If this list is empty on a succesful call, we have reached the end
    #Of markets that match our critera and break the loop
    if not row_list:
        print('Query complete, begining save to database')
        break
    
    #Adding the data to the events list, then resuming the loop
    for event in row_list:
        events_list.append(event)
    row_list = []
    offset += return_amount
    print(f'{offset} records collected')

#%%
#==========================================================================
#Formatting data
#==========================================================================

#Separating out the markets for their own table
market_list = extract_children(events_list, 'markets')
market_df = pd.DataFrame(market_list)
market_df = market_df.drop(columns=['outcomes', 'outcomePrices', 'clobTokenIds', 'umaResolutionStatuses', 'clobRewards', 'groupItemRange', 'feeSchedule'])

#Separating out tags for their own table
tag_list = extract_children(events_list, 'tags')
tag_df = pd.DataFrame(tag_list)

#Formatting the event data
event_df = pd.DataFrame(events_list)
event_df = event_df.drop(columns=['eventMetadata', 'tags', 'series', 'markets'])

#%%
#==========================================================================
#Adding the dataframes to the database
#==========================================================================

#Creating the events table
create_table(conn, event_df, event_table_name, 'id' )
event_df.to_sql(event_table_name, conn, if_exists='append', index=False)

#Creating the markets table
create_table(conn, market_df, market_table_name, 'id', 'event_id', event_table_name )
market_df.to_sql(market_table_name, conn, if_exists='append', index=False)

#Creating tags table
create_table(conn, tag_df, tag_table_name, 'id', 'event_id', event_table_name )
tag_df.to_sql(tag_table_name, conn, if_exists='append', index=False)

print('Script Complete!')