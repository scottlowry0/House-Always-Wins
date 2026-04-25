# -*- coding: utf-8 -*-
"""
Created on Fri Apr 24 16:05:51 2026

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
config_file = 'collect_config.toml'

with open(config_file,'rb') as fh:
    config = tomllib.load(fh)    
    holders_api = config['holders_api']
    activity_api = config['activity_api']
    db_name = config['db_name']
    event_table_name = config['table_names']['event_table_name']
    market_table_name = config['table_names']['market_table_name']
    tag_table_name = config['table_names']['tag_table_name']
    tag_bridge_table_name = config['table_names']['tag_bridge_table_name']
    transaction_table_name = config['table_names']['transaction_table_name']

return_amount = 100
offset = 0
conn = sqlite3.connect(db_name)
tag_slugs = ['derivatives']

#==========================================================================
#CALLING TOP HOLDERS API
#==========================================================================
    
event_ids = get_event_ids_by_tags(conn, tag_slugs, event_table_name, tag_bridge_table_name, tag_table_name)

for event in event_ids:
    params = {
        'limit' = return_amount,
        'eventId' = event
        'offset' = offset
        }
