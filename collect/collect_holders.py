# -*- coding: utf-8 -*-
"""
Created on Sun Apr 26 17:51:37 2026

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
    condition_id = config['derivative_market']
    underlying_condition = config['market_underlying']

return_amount = 100
offset = 0
#==========================================================================
#CALLING API
#==========================================================================
params = {
        'market': condition_id,
        'status': 'ALL',
        'sortBy': 'TOTAL_PNL',
        'offset': 0,
        'limit': 500,
        }

positions_list = call_api(positions_api, params)

#==========================================================================
#FORMATTING DATA
#==========================================================================
position_not_nested = extract_children(positions_list, 'positions')
positions_df = pd.DataFrame(position_not_nested)
pd.to_pickle(positions_df, 'holders.pkl')
