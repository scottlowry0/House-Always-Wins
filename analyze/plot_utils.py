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
    
#==========================================================================
#plot_two_lines
#plots two separate markets on the same graph
#==========================================================================
    
def plot_two_dataframes(df1: pd.DataFrame, 
                        df2: pd.DataFrame, 
                        xval: str, 
                        yval: str, 
                        label1: str = "Market 1", 
                        label2: str = "Market 2",
                        title: str = "Dual Market Comparison"):
    
    plt.figure(figsize=(10, 5))
    
    # Plot the first dataframe
    sns.lineplot(data=df1, x=xval, y=yval, label=label1, linewidth=2, errorbar=None)
    
    # Plot the second dataframe on the same chart
    sns.lineplot(data=df2, x=xval, y=yval, label=label2, linewidth=2, errorbar=None)
    
    plt.title(title, fontsize=14)
    plt.xlabel(xval, fontsize=12)
    plt.ylabel(yval, fontsize=12)
    
    plt.xticks(rotation=45)
    plt.legend() 
    plt.tight_layout()
    plt.show() # or plt.savefig('your_filename.png')