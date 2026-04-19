# -*- coding: utf-8 -*-
"""
Created on Sun Apr 19 13:03:33 2026

@author: Scott Lowry
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

return_amount = 10
offset = 0
conn = sqlite3.connect(db_name)

#%%
#==========================================================================
#Calling API
#==========================================================================

params = {
    'limit': return_amount,
    'offset': offset,
}
tag_list = call_api(api, params, max_returns=1)



#%%
#==========================================================================
#Calling  Event API
#==========================================================================
   
params = {
    'limit': return_amount,
    'tag_slug': tag_slug,
    'include_chat': include_chat,
    'offset': offset,
    'active': active,
    'closed': closed
}

events_list = call_api(api, params, max_returns=1)