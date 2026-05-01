# -*- coding: utf-8 -*-
"""
Created on Sun Apr 12 14:27:25 2026

@author: Scott Lowry

A Script containing helper functions for the House-Always-Wins library
"""

#Imports and Config
import pandas as pd
import sqlite3
import requests
import sys

#==========================================================================
#get_columns
#A function for getting the column names and data types for SQL strings
#expects a dataframe, then returns a list with col name
#and data types
#==========================================================================
def get_columns(dataframe):
    columns = []
    for col in dataframe:
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
#For primary keys, give a list of primary keys
#For foreign keys give a list of tuples
#for the tuples the first entry is the foreign key
#and the second entry is the name of the parent table
#==========================================================================
def create_table(conn: sqlite3.Connection, 
                 dataframe: pd.DataFrame, 
                 table_name: str, 
                 primary_keys: list|None = None, 
                 foreign_keys: list[tuple[str, str]]|None = None, 
    ):
    
    #Getting column names and data types
    column_names = get_columns(dataframe)
    
    #Adds primary keys if provided
    if primary_keys:
        pk_str = ', '.join([f'"{pk}"' for pk in primary_keys])
        column_names.append(f'PRIMARY KEY ({pk_str})')
    
    #Adds a foreign key and parent table if provided
    if foreign_keys:
        for foreign_key, parent_table in foreign_keys:
            column_names.append(f"""
                            FOREIGN KEY ("{foreign_key}") 
                            REFERENCES "{parent_table}"("id") 
                            ON DELETE CASCADE
                            """)
    #Joining the list together then excuting statement to create table
    #If the table does not yet exist
    table_str = ',\n'.join(column_names)
    create_table = f"CREATE TABLE IF NOT EXISTS {table_name} (\n    {table_str}\n);"
    conn.execute(create_table)
    

#==========================================================================
#extract_children
#A function for extracting data for child tables
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
#==========================================================================
#call_api
#A function for calling the polymarket api
#Required Arguements: API endpoint and a dict containing parameters
#Optional Arguements: the amount of returns per call (polymarket max at 500 per call)
#to edit specific parameters, use config.toml. for more information on
#what each api parameter does, see the readme
#==========================================================================
def call_api(api: str, params: dict, **kwargs):
    offset = params['offset']
    max_returns = kwargs.get('max_returns', 5000)
    return_amount = params['limit']
    return_list = []
    total_records = 0
    while offset <= max_returns:
        try:
            response = requests.get(api, params=params)
            response.raise_for_status()
            retry = 0
        
        except Exception as e:
                print('Exception',e)
                print(getattr(response, 'status_code', 'No status code')) 
                print(getattr(response, 'text', 'No text'))
                if retry < 3:
                    retry += 1
                    print('\nRetry',retry)
                    continue
                else:
                    print('\nMaximum retries exceeded')
                    sys.exit()
                    
        
        #Formatting a succesful call
        row_list = response.json()
        
        #If this list is empty on a succesful call, we have reached the end
        #Of markets that match our critera and break the loop
        if not row_list:
            print('Query complete, begining save to database')
            break
        
        #Adding the data to the return list, then resuming the loop
        for result in row_list:
            return_list.append(result)
        print(f'{len(row_list)} records collected')
        total_records += len(row_list) 
        print(f'total records: {total_records}')
        row_list = []
        offset += return_amount
        params['offset'] = offset
        
    if offset >= max_returns:
        print('API Call ended due to max calls reached. If not all records have been pulled, increase max_returns')
    return return_list
    
#==========================================================================
#upsert_data
#A function for adding or updating data within an existing table
#Required Arguements: API endpoint and a dict containing parameters
#Optional Arguements: the amount of returns per call (polymarket max at 500 per call)
#to edit specific parameters, use config.toml. for more information on
#what each api parameter does, see the readme
#==========================================================================
def upsert_data(conn: sqlite3.Connection, 
                dataframe: pd.DataFrame, 
                table_name: str, 
                primary_keys: list | None = None
                ):
    
        #Removing columns in the dataframe not currently in the table
        cursor = conn.execute(f"PRAGMA table_info({table_name})")
        db_cols = [row[1] for row in cursor.fetchall()]
        valid_cols = [col for col in dataframe.columns if col in db_cols]
        dataframe = dataframe[valid_cols]
    
    
        if primary_keys:
           temp_table = f"{table_name}_temp"
           dataframe.to_sql(temp_table, conn, if_exists='replace', index=False)
           
           #Finds non-primary key columns for updating
           data_cols = [col for col in dataframe.columns if col not in primary_keys]
           
           #Format the list into string for the ON CONFLICT line
           pk_sql_str = ', '.join([f'"{pk}"' for pk in primary_keys])
           
           col_names = ', '.join([f'"{col}"' for col in dataframe.columns])
           
           #If there are non primary key columns, sort them out for updating
           if data_cols:
                update_cols = [f'"{col}" = excluded."{col}"' for col in data_cols]
                update_str = ',\n        '.join(update_cols)
                
                skip_rules = [f'{table_name}."{col}" IS NOT excluded."{col}"' for col in data_cols]
                where_str = '\n        OR '.join(skip_rules)
                
                upsert_sql = f"""
                    INSERT INTO {table_name} ({col_names}) 
                    SELECT {col_names} FROM {temp_table}
                    WHERE 1
                    ON CONFLICT({pk_sql_str}) DO UPDATE SET 
                    {update_str}
                    WHERE {where_str};
                """
           #If there are no non-primary key columns
           else:
                upsert_sql = f"""
                    INSERT INTO {table_name} ({col_names}) 
                    SELECT {col_names} FROM {temp_table}
                    WHERE 1
                    ON CONFLICT({pk_sql_str}) DO NOTHING;
                """
           with conn:
                conn.execute(upsert_sql)
                conn.execute(f"DROP TABLE {temp_table};")
        #If there are no primary key columns in the table    
        else:
            dataframe.to_sql(table_name, conn, if_exists='append', index=False)
            
#==========================================================================
#get_event_ids_by_tag
#A function for getting all conditionIDs from the markets table by a specific tag
#Required Arguements: The plain text tag slug list (i.e ['politics', 'sports'])

#==========================================================================            
def get_event_ids_by_tags(conn: sqlite3.Connection, 
                              tag_slugs: list,
                              event_table_name: str,
                              tag_bridge_table_name: str,
                              tag_table_name: str) -> list:
    
    #Builds query that oins with the events and tags tables,
    #Then searches for records with matching tags
    placeholders = ', '.join(['?'] * len(tag_slugs))
    query = f"""
        SELECT DISTINCT e.id
        FROM {event_table_name} e
        JOIN {tag_bridge_table_name} et ON e.id = et.event_id
        JOIN {tag_table_name} t ON et.id = t.id
        WHERE t.slug IN ({placeholders});  
    """
    
    #Running the query, then returning the list
    df = pd.read_sql(query, conn, params=tuple(tag_slugs))   
    return df['id'].dropna().tolist()

#==========================================================================
#create_tag_view
#A function for getting all conditionIDs from the markets table by a specific tag
#Required Arguements: The plain text tag slug list (i.e ['politics', 'sports'])
#========================================================================== 
def create_tag_view(conn: sqlite3.Connection,
                    view_name: str,
                    target_tag: str,
                    market_table_name: str,
                    tag_bridge_table_name: str,
                    tag_table_name: str,):
    conn.execute(f"""
        CREATE VIEW IF NOT EXISTS {view_name} AS
        SELECT 
            m.id, 
            m.question, 
            m.conditionId, 
            m.slug, 
            m.description, 
            m.volume
        FROM {market_table_name} m
        JOIN {tag_bridge_table_name} et ON m.event_id = et.event_id
        JOIN {tag_table_name} t ON et.id = t.id
        WHERE t.slug = '{target_tag}'
    """)
    conn.commit()
           
      
#==========================================================================
#merge_alternating_dict
#A function for merging dictionaries
#good for where the api returns lists by token 
#(i.e a yes token list and a no token list) 
#==========================================================================     
def merge_alternating_dicts(dict_list, num_groups=2):
    # Create a list holding empty dictionaries for the number of groups you want
    merged_results = [{} for _ in range(num_groups)]
    
    # Loop through the list and update the corresponding dictionary
    for i, d in enumerate(dict_list):
        # i % 2 will alternate between 0 and 1, routing the dict to the right group
        merged_results[i % num_groups].update(d)
        
    return merged_results


#==========================================================================
#create_market_transactions_view
#Creates a view of all transactions associated with a specific market 
#by joining the market table
#==========================================================================
def create_market_transactions_view(conn: sqlite3.Connection, 
                                    view_name: str, 
                                    transaction_table: str, 
                                    condition_id: str, 
                                    ):
    
    conn.execute(f"""
         CREATE VIEW IF NOT EXISTS "{view_name}" AS
         SELECT *
         FROM {transaction_table}
         WHERE conditionId = '{condition_id}'
     """)
    conn.commit()