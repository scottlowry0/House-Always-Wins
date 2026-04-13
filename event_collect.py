# -*- coding: utf-8 -*-
"""
Created on Sun Apr 12 13:09:01 2026

@author: Scott Lowry

A first draft script for using the polymarket API to build a table of events

*********************************************************
THIS SCRIPT WILL DROP TABLES FROM THE DATABSE BEING USED
*********************************************************
"""

#Imports
import pandas as pd
import requests
import sqlite3
import sys

#==========================================================================
#CONFIG
#==========================================================================
api = 'https://gamma-api.polymarket.com/events'

#Names for DB, and tables
db_name = 'markets_test.db'
event_table_name = 'events_iran_test'
market_table_name = 'market_iran_test'
tag_table_name = 'tag_iran_test'
conn = sqlite3.connect(db_name)

#offset should always start as 0
#increment amount is the number of results that each call will return
#max returns is the max number of events to return
offset = 0
return_amount = 5
max_returns = 1500

#only calls markets with the selected tag slug. Tag slugs are a text tag
#using number tag ids will fail
tag_slug = 'iran'

#Determines if the script calls inactive and closed markets
#true and false will call both
active = ['true', 'false']
closed = ['true', 'false']

#other parameters
include_chat = 'false'
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
market_list = [
    {**market, 'event_id': event.get('id')} 
    for event in events_list 
    for market in event.get('markets', [])
]
colnames = market_list[0]
datarows = market_list[1:]
market_df = pd.DataFrame(columns=colnames, data=datarows)
market_df = market_df.set_index(['id', 'event_id'])
market_df = market_df.drop(columns=['outcomes', 'outcomePrices', 'clobTokenIds', 'umaResolutionStatuses', 'clobRewards'])

#Separating out tags for their own table
tag_list = [
    {**tag, 'event_id': event.get('id')} 
    for tag in events_list
    for tag in event.get('tags', [])
]
colnames = tag_list[0]
datarows = tag_list[1:]
tag_df = pd.DataFrame(columns=colnames, data=datarows)
tag_df = tag_df.set_index(['id', 'event_id'])

#Formatting the event data
colnames = events_list[0]
datarows = events_list[1:]
event_df = pd.DataFrame(columns=colnames, data=datarows)
event_df = event_df.drop(columns=['eventMetadata', 'tags', 'series', 'markets'])

#%%
#==========================================================================
#Adding the dataframes to the database
#==========================================================================
with conn:
    conn.executescript(f"""
    DROP TABLE IF EXISTS {market_table_name};
    DROP TABLE IF EXISTS {tag_table_name};
    DROP TABLE IF EXISTS {event_table_name};
""")
event_df.to_sql(event_table_name , conn, index=False)
tag_df.to_sql(tag_table_name, conn, index=False)
market_df.to_sql(market_table_name, conn, index=False)

