# -*- coding: utf-8 -*-
"""
Created on Fri Apr 24 16:05:51 2026

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
    activity_api = config['activity_api']
    
return_amount = 100
offset = 0
condition_id = '0x0b4cc3b739e1dfe5d73274740e7308b6fb389c5af040c3a174923d928d134bee'
user = '0xb03b826a4fc9893b35d3ddf4f11be824525b6ca1'

#==========================================================================
#CALLING API
#==========================================================================
    
params = {
        'user': user,
        'market': condition_id,
        'offset': 0,
        'limit': 100,
        }

user_data = call_api(activity_api, params, max_returns=3000)
if not user_data:
    sys.exit('No data collected')
    
#==========================================================================
#Formatting Data
#==========================================================================
#%%
market_1 = pd.DataFrame(user_data)

#Converting from Unix time and sorting data to chronological order
market_1['timestamp'] = pd.to_datetime(market_1['timestamp'], unit='s')
market_1 = market_1.sort_values('timestamp').reset_index(drop=True)

#Making sells negative volume
market_1.loc[market_1['side']== 'SELL', 'size'] *= -1

#Separating out Yes and No positions
market_yes = market_1.loc[market_1['outcome']=='Yes'].copy()
market_no = market_1.loc[market_1['outcome']=='No'].copy()

#Finding cumulative volume sums for plotting
market_yes['total_position'] = market_yes['size'].cumsum()
market_no['total_position'] = market_no['size'].cumsum()

#Finding dollar amount spent or gained per transaction
market_yes['transaction_cost'] = (market_yes['size'] *-1) * market_yes['price']
market_yes['total_earnings'] = market_yes['transaction_cost'].cumsum()
market_no['transaction_cost'] = (market_no['size'] *-1) * market_no['price']
market_no['total_earnings'] = market_no['transaction_cost'].cumsum()
#==========================================================================
#Plotting Data
#==========================================================================
#%%
plot_over_time(market_yes, 'timestamp', 'total_position', 'Date', 'Volume')
plot_over_time(market_yes, 'timestamp', 'total_earnings', 'Date', 'Total Earnings')
