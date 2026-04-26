# -*- coding: utf-8 -*-
"""
Created on Sun Apr 26 15:05:57 2026

@author: Scott Lowry
"""
#Imports and config
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns

#==========================================================================
#plot_over_time
#plots a users cumulative position in a market over time
#==========================================================================
def plot_over_time(dataframe: pd.DataFrame, 
                   xval: str, 
                   yval: str, 
                   xlabel: str,
                   ylabel: str,
                   ):
    title = f" User {dataframe['name'].iloc[0]}: Outcome {dataframe['outcome'].iloc[0]} in market {dataframe['title'].iloc[0]} "
    fig_title = f"{dataframe['name'].iloc[0]}_{dataframe['outcome'].iloc[0]}_{dataframe['slug'].iloc[0]}_{yval}"
    plt.figure(figsize=(10, 5))
    sns.lineplot(data=dataframe, x=xval, y=yval, drawstyle='steps-post', color='tab:orange', linewidth=2)
    plt.title(title, fontsize=14)
    plt.xlabel(xlabel, fontsize=12)
    plt.ylabel(ylabel, fontsize=12)
    ax = plt.gca()
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f'{fig_title}_figure.png')