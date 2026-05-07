# -*- coding: utf-8 -*-
"""
Created on Wed May  6 23:56:54 2026

@author: Scott Lowry
"""
#Imports
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

#importing data
underlying_yes = pd.read_pickle('underlying_yes.pkl')
underlying_no = pd.read_pickle('underlying_no.pkl')
derivative_yes = pd.read_pickle('derivative_yes.pkl')
derivative_no = pd.read_pickle('derivative_no.pkl')

df_list = [underlying_yes, underlying_no, derivative_yes, derivative_no]
#=========================================================================
#CLEANING DATA
#=========================================================================
#Converting from Unix time
derivative_yes['timestamp'] = pd.to_datetime(derivative_yes['timestamp'], unit = 's')
derivative_yes.set_index('timestamp', inplace=True)
derivative_yes.sort_values('timestamp', ascending=True, inplace=True)
derivative_no['timestamp'] = pd.to_datetime(derivative_no['timestamp'], unit = 's')
derivative_no.set_index('timestamp', inplace=True)

#making sure all dfs are in time order
derivative_no.sort_values('timestamp', ascending=True, inplace=True)
derivative_yes.sort_values('timestamp', ascending=True, inplace=True)
underlying_no.sort_values('timestamp', ascending=True, inplace=True)
underlying_yes.sort_values('timestamp', ascending=True, inplace=True)

#Calculating the cumulative sum for each user
for df in df_list:
    df['cumulative_sum'] = df.groupby('proxyWallet')['size'].cumsum()

#=========================================================================
#GRAPHING FUNCTIONS
#=========================================================================    
def process_side_grouped(df_und, df_der, side_label):
    # Get final cumulative positions per wallet
    und_final = df_und.groupby('proxyWallet').tail(1)[['proxyWallet', 'cumulative_sum', 'name']]
    der_final = df_der.groupby('proxyWallet').tail(1)[['proxyWallet', 'cumulative_sum']]

    merged = pd.merge(und_final, der_final, on='proxyWallet', suffixes=('_und', '_der'))
    
    # Calculate combined total to find top 10 
    merged['total_sum'] = merged['cumulative_sum_und'] + merged['cumulative_sum_der']
    top_10 = merged.nlargest(10, 'total_sum').copy()

    # Create short labels
    top_10['label'] = top_10.apply(
        lambda r: str(r['name'])[:10] if pd.notna(r['name']) and str(r['name']).strip() != '' 
        else str(r['proxyWallet'])[:10], axis=1
    )

    #Setup positions for side-by-side bars
    x = np.arange(len(top_10['label'])) 
    width = 0.35

    fig, ax = plt.subplots(figsize=(14, 7))
    
    rects1 = ax.bar(x - width/2, top_10['cumulative_sum_und'], width, label='Underlying', color='skyblue', edgecolor='black')
    rects2 = ax.bar(x + width/2, top_10['cumulative_sum_der'], width, label='Derivative', color='orange', edgecolor='black')

    # Add text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Total Position Size (Shares)')
    ax.set_title(f'Top 10 Users Comparison: {side_label} Side', fontsize=16)
    ax.set_xticks(x)
    ax.set_xticklabels(top_10['label'], rotation=45, ha='right')
    ax.legend()

    fig.tight_layout()
    plt.savefig(f'{side_label}_side_by_side.png')
    plt.show()

def plot_user_comparison(df_und, df_der, wallet_address, side_label, figure_name):
    # Resetting index
    user_und = df_und[df_und['proxyWallet'] == wallet_address].copy().reset_index()
    user_der = df_der[df_der['proxyWallet'] == wallet_address].copy().reset_index()

    plt.figure(figsize=(12, 6))

    #Plotting underlying market
    if not user_und.empty:
        plt.step(user_und['timestamp'], user_und['cumulative_sum'], 
                 where='post', 
                 label='Underlying Market', 
                 color='blue', 
                 linewidth=4,    
                 alpha=0.4)       

    #Plotting derivative
    if not user_der.empty:
        user_der['timestamp'] = pd.to_datetime(user_der['timestamp'])
        user_der = user_der.sort_values('timestamp')
        plt.step(user_der['timestamp'], user_der['cumulative_sum'], 
                 where='post', 
                 label='Derivative Market', 
                 color='darkorange', 
                 linewidth=1.5,   
                 alpha=1.0)       

    #Formatting
    plt.title(f"Position Comparison: {figure_name}\n({side_label} Side)")
    plt.xlabel('Time')
    plt.ylabel('Position Size (Shares)')
    plt.legend()
    plt.grid(True, linestyle=':', alpha=0.6)
    plt.tight_layout()
    
    plt.savefig(f"{figure_name}_{side_label}_comparison.png")
    plt.show()


#=========================================================================
#CREATING GRAPHS
#=========================================================================

#Creating plots
process_side_grouped(underlying_yes, derivative_yes, "Yes")
process_side_grouped(underlying_no, derivative_no, "No")

plot_user_comparison(underlying_yes,
                     derivative_yes,
                     '0x9d84ce0306f8551e02efef1680475fc0f1dc1344', 
                     'yes', 
                     'User_ImJustKen')


plot_user_comparison(underlying_yes,
                     derivative_yes,
                     '0xdc09c0fdf3e8b550faaaa96e0e7c74ef938a9024', 
                     'yes', 
                     'User_tjdlps')

plot_user_comparison(underlying_no,
                     derivative_no,
                     '0xe598435df0cdf5d22bdd5082d557f75f9180a0a8', 
                     'no', 
                     'User_bamesjond')

plot_user_comparison(underlying_no,
                     derivative_no,
                     '0x22e4248bdb066f65c9f11cd66cdd3719a28eef1c', 
                     'no', 
                     'User_Professional_Punter')