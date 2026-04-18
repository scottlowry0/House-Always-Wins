# -*- coding: utf-8 -*-
"""
Created on Sun Apr 12 14:27:25 2026

@author: Scott Lowry

A Script containing helper functions for the House-Always-Wins library
"""

#Imports and Config
import pandas as pd
import sqlite3

#==========================================================================
#get_columns
#A function for getting the column names and data types
#expects a dataframe, then returns a list with col name
#and data types
#==========================================================================
def get_columns(dataframe, primary_key: str):
    columns = []
    for col in dataframe:
        if col == primary_key:
            columns.append(f'"{col}" TEXT PRIMARY KEY')
            continue
        dtype = str(dataframe[col].dtype).lower()
        if 'int' in dtype or 'bool' in dtype:
            sql_type = 'INTEGER'
        elif 'float' in dtype:
            sql_type = 'REAL'
        else:
            # Handles strings ('object'), datetimes, and mixed types
            sql_type = 'TEXT'
        columns.append(f'"{col}" {sql_type}')
        
    return columns
#==========================================================================
#create_table
#A function for creating tables from dataframes that need to be linked
#expects a db connection, dataframe, table name, and keys and relations
#then creates a table
#==========================================================================
def create_table(conn: sqlite3.Connection, 
                 dataframe: pd.DataFrame, 
                 table_name: str, 
                 primary_key: str, 
                 foreign_key: str|None = None, 
                 parent_table: str|None =None
                 ):
    #Getting column names and data types
    column_names = get_columns(dataframe, primary_key)
    
    #Adds a foreign key and parent table if provided
    if foreign_key and parent_table:
        column_names.append(f"""
                        FOREIGN KEY ("{foreign_key}") 
                        REFERENCES "{parent_table}"("id") 
                        ON DELETE CASCADE
                        """)
    #Joining the list together then excuting statement to create tables
    table_str = ',\n'.join(column_names)
    create_table = f"CREATE TABLE {table_name} (\n    {table_str}\n);"
    conn.execute("PRAGMA foreign_keys = OFF;")
    with conn:
        conn.execute(f"DROP TABLE IF EXISTS {table_name};")
        conn.execute(create_table)
    conn.execute("PRAGMA foreign_keys = ON;")


#==========================================================================
#extract_children
#An unfortunately named function for extracting data for child tables
#Parent list is the list containing one or more dictionaries that need extracting
#Child key is the dictionary key for the list of dictionaries you are extracting
#i.e if you want the markets give it markets as the arguement
#The event id for the data is appended on for later use a foreign key
#==========================================================================
def extract_children(parent_list: list, child_key: str):
    return [
        {**child, 'event_id': event.get('id')}
        for event in parent_list
        for child in event.get(child_key, [])
    ]