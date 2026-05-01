# -*- coding: utf-8 -*-
"""
Created on Tue Apr 28 12:55:18 2026

@author: Scott Lowry
"""

#Imports
import tomllib
import pandas as pd
from collect_utils import *
from plot_utils import *

#==========================================================================
#CONFIG
#==========================================================================
config_file = 'collect_config.toml'

with open(config_file,'rb') as fh:
    config = tomllib.load(fh)    
    activity_api = config['activity_api']
    market_underlying = config['market_underlying']
    market_derivative = config['market_derivative']
    
return_amount = 100
offset = 0
top_holders = pd.read_pickle('top_holders.pkl')
transactions = pd.read_pickle('transactions.pkl')

#==========================================================================
#Formatting Data
#==========================================================================
#%%
#Splitting out the top holders by outcome
in_both_yes = top_holders.loc[(top_holders['In_Both'] == True) & (top_holders['outcome'] == 'Yes')].copy()
in_both_no =  top_holders.loc[(top_holders['In_Both'] == True) & (top_holders['outcome'] == 'No')].copy()

#Splitting transactions df by outcome & market
transactions_underlying_yes = transactions.loc[(transactions['outcome'] == 'Yes') & (transactions['conditionId'] == market_underlying)].copy()
transactions_underlying_no = transactions.loc[(transactions['outcome'] == 'No') & (transactions['conditionId'] == market_underlying)].copy()

transactions_derivative_yes = transactions.loc[(transactions['outcome'] == 'Yes') & (transactions['conditionId'] == market_derivative)].copy()
transactions_derivative_no = transactions.loc[(transactions['outcome'] == 'No') & (transactions['conditionId'] == market_derivative)].copy()
#==========================================================================
#Analyzing Market Trends
#==========================================================================
#%%
market_underlying_yes = transactions_underlying_yes
market_underlying_no = transactions_underlying_no
market_derivative_yes = transactions_derivative_yes
market_derivative_no = transactions_derivative_no
market_df_list = [market_underlying_yes, market_underlying_no, market_derivative_yes, transactions_derivative_no]

#Finding cumulative volume sums for plotting
for df in market_df_list:
    df['total_position'] = df['size'].cumsum()

#Finding dollar amount spent or gained per transaction
for df in market_df_list:
    df['transaction_cost'] = (df['size'] *-1) * df['price']
    df['total_earnings'] = df['transaction_cost'].cumsum()

#Plotting market trends
agg_funcs = {
    'total_position': 'mean',
    'name': 'first',
    'outcome': 'first',
    'slug': 'first',
    'title': 'first'
}

#Regrouping dfs into 30 min intervals for plotting
market_underlying_yes = market_underlying_yes.set_index('timestamp').resample('30min').agg(agg_funcs).reset_index()
market_underlying_no = market_underlying_no.set_index('timestamp').resample('30min').agg(agg_funcs).reset_index()
market_derivative_yes = market_derivative_yes.set_index('timestamp').resample('30min').agg(agg_funcs).reset_index()
market_derivative_no = market_derivative_no.set_index('timestamp').resample('30min').agg(agg_funcs).reset_index()
#%%
for df in market_df_list:
    plot_over_time(df, 'timestamp', 'total_position', 'Time', 'Volume')

# # 2. Resample the specific variables used in your dual plot
# market_derivative_yes = market_derivative_yes.set_index('timestamp').resample('30min').agg(agg_funcs).reset_index()
# market_underlying_yes = market_underlying_yes.set_index('timestamp').resample('30min').agg(agg_funcs).reset_index()

plot_two_dataframes(market_derivative_yes,
                    market_underlying_yes, 
                    'timestamp', 
                    'total_position',
                    'derivative market',
                    'underlying market',
                    'Market Comparison: No side' 
                    )




