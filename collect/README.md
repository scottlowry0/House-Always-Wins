# Collect Module

The collect module builds a database of Polymarket data including tags, markets, events, and transactions.
This document outlines what each of the scripts in the collect module does and what order they must be used in. Scripts are numbered in the order that they must be run to recreate the analysis of this repository.

Scripts numbered 0 do not need to be run but must be present for the module to work. All scripts that add to the database in this module will insert data into existing tables without duplicating records so they are safe to run multiple times with queries that might pull the same records twice.

## 0. Config.toml
This file contains the config for the entire repository. It must be present in the top level of the directory as it is shared between the collect and analyze modules.
Specific alterations that need to be made to this file for each script will be listed under their respective headings.

## 0. Collect_utils.py
This script is a functions library relied upon by the collect module.
It needs to be present in the collect directory but does not need to be run.
Documentation for each of the specific functions in the directory can be found above each function in the file.

## 1. Collect_tags.py
This script calls the Polymarket tags API endpoint to collect all the tags on Polymarket.
It then creates the database if it does not exist, creates the tags table, and inserts the data into the table.
As the tags table is the parent of the tags_event_bridge table created in the next script, this script must be run first to ensure that the relationship is established correctly.

## 2. Collect_event.py
This script calls the Polymarket events API endpoint to collect all events under the specified tags in the config file.
The tags should be placed in the config in the form of a list.
The default list of tags is [`pop-culture`, `derivatives`, `syracuse`]. This query can take some time depending on the number of tags given to the script and how many events exist under each tag.
The script then builds the events table for all events, the markets table for markets, and the tags_event_bridge table to handle the many-to-many relationship between tags and events.

## 3. Collect_holders.py
This script cycles through a selected market and pulls all users by their largest position.
This script then outputs that dataframe as a pickle file.
This file is used later to collect transaction data for users.

## 4. Collect_transactions.py
This script cycles through the pickle file created by the previous script and finds all of the transactions for each user in that file for the selected market.
To recreate this analysis, this file must be run twice.
The first run through the market_to_collect variable in the config file should be set to the conditionId for the derivative market.
The second run of the script the market_to_collect in the config file should be set to the underlying market.
The same holder pickle collecting users who participated in the derivative market should be used both times.
All data is output into the transactions table in the database.

As an aside, this script is an incredibly inefficient way to query the API however it is also the only way to collect all of the desired transaction data.
The transaction endpoint for markets is capped at 3,000 records and there is not a timestamp argument to use as a workaround.
Therefore, the only way to collect all the records for a given market is to loop through the user transaction history that does have a timestamp argument, allowing for the collection of all of the data.
The major caveat of this approach is that it will not work with larger markets.
Once markets get beyond a few thousand users the script takes too long to collect all of the relevant records.
Additionally, this script does not currently have any logic to fail gracefully so if a call fails more than three times the script will crash and need to be restarted from the beginning.
If you intend to use this script to collect transactions from a market other than the ones analyzed in this repository, try to limit its use to small to medium size markets.
For reference, the market used in this analysis has 1,300 users and takes around 20 minutes to collect all of the transactions.
