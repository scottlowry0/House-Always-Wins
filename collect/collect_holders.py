# -*- coding: utf-8 -*-
"""
Created on Sun Apr 26 17:51:37 2026

@author: Scott Lowry
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
    positions_api = config['positions_api']
    activity_api = config['activity_api']
    
return_amount = 100
offset = 0
condition_id = '0xc1588218a290e61ef35545553b157f76702cc3c61f7f1d309a9c68a49cbc29fd'
underlying_condition = '0x0b4cc3b739e1dfe5d73274740e7308b6fb389c5af040c3a174923d928d134bee'

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

#Filtering out wallets with a small position in the market
top_holders = positions_df.loc[positions_df['totalBought'] >= 1000].copy()
    
#==========================================================================
#CHECKING FOR PARTICIPATION IN UNDERLYING MARKET
#==========================================================================
#Looping through the top holders and checking if they participated
#In the market for the underlying asset
#If they did, marking participation as TRUE
for index in top_holders.index:
    wallet = top_holders.loc[index, 'proxyWallet']
    check_list = []
    params = {
            'market': underlying_condition,
            'user': wallet,
            'limit': 1,
            'offset': 0,
            }
    check_list = call_api(activity_api, params, max_returns=1)
    top_holders.loc[index, 'In_Both'] = bool(check_list)

