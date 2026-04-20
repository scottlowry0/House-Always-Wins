# -*- coding: utf-8 -*-
"""
Created on Sat Apr 18 17:28:25 2026

@author: Scott Lowry

A Script for creating a table of all of the tags on polymarket

As of Apr 2026 there are about 6,000 tags on polymarket
Expect this script to take a while
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
    api = config['tag_api']
    db_name = config['db_name']
    tag_table_name = config['table_names']['tag_table_name']

return_amount = 100
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
tag_list = call_api(api, params, max_returns=10000)


#%%
#==========================================================================
#Formatting Data
#==========================================================================
tag_df = pd.DataFrame(tag_list)
#Creating tags table
create_table(conn, tag_df, tag_table_name, ['id'])
tag_df.to_sql(tag_table_name, conn, if_exists='append', index=False)
