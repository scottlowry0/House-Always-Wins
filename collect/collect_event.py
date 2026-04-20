# -*- coding: utf-8 -*-
"""
Created on Sun Apr 12 13:09:01 2026

@author: Scott Lowry

A first draft script for using the polymarket API to build a table of events

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
    api = config['event_api']
    db_name = config['db_name']
    tag_slug = config['tag_slug']
    include_chat = config['include_chat']
    event_table_name = config['table_names']['event_table_name']
    market_table_name = config['table_names']['market_table_name']
    tag_table_name = config['table_names']['tag_table_name']
    tag_bridge_table_name = config['table_names']['tag_bridge_table_name']
    active = config['market_status']['active']
    closed = config['market_status']['closed']

return_amount = 100
offset = 0
conn = sqlite3.connect(db_name)


#%%
#==========================================================================
#Calling API
#==========================================================================
   
params = {
    'limit': return_amount,
    'tag_slug': tag_slug,
    'include_chat': include_chat,
    'offset': offset,
    #'active': active,
    #'closed': closed
}

events_list = call_api(api, params)

#%%
#==========================================================================
#Formatting data
#==========================================================================

#Separating out the markets for their own table
market_list = extract_children(events_list, 'markets')
market_df = pd.DataFrame(market_list)
market_df = market_df.drop(columns=['outcomes', 'outcomePrices', 'clobTokenIds', 'umaResolutionStatuses', 'clobRewards', 'groupItemRange', 'feeSchedule'], errors='ignore')

#Separating out tags for their own table
tag_list = extract_children(events_list, 'tags')
tag_df = pd.DataFrame(tag_list)
#All of the tag data is stored in the parent tags table
#Dropping everything else since the bridge just needs the relations
tag_df = tag_df[['id', 'event_id']]

#Formatting the event data
event_df = pd.DataFrame(events_list)
event_df = event_df.drop(columns=['eventMetadata', 'tags', 'series', 'markets', 'eventCreators'], errors='ignore')

#%%
#==========================================================================
#Adding the dataframes to the database
#==========================================================================

#Creating the events table
create_table(conn, event_df, event_table_name, ['id'] )
#event_df.to_sql(event_table_name, conn, if_exists='append', index=False)
upsert_data(conn, event_df, event_table_name, ['id'])

#Creating the markets table
create_table(
             conn, 
             market_df, 
             market_table_name, 
             ['id'], 
             [('event_id', event_table_name )]
             )
#market_df.to_sql(market_table_name, conn, if_exists='append', index=False)
upsert_data(conn, market_df, market_table_name, ['id'])

#Creating tags-events bridge table table
create_table(conn, 
             tag_df, 
             tag_bridge_table_name,
             primary_keys=['event_id', 'id'], 
             foreign_keys=[('event_id', event_table_name),
              ('id', tag_table_name)] )
#tag_df.to_sql(tag_table_name, conn, if_exists='append', index=False)
upsert_data(conn, tag_df, tag_bridge_table_name, primary_keys=['event_id', 'id'])

print('Script Complete')
