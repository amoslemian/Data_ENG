from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

# Code for ETL operations on Country-GDP data

# Importing the required libraries

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''

    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./etl_project_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')   

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
   
    headers = {"User-Agent": "Mozilla/5.0"}
    page = requests.get(url, headers=headers)
    tables = pd.read_html(page.text)
    data=tables[0]
    
    df = data.loc[:, ["Bank name", data.columns[2]]]

    # rename them to match table_attribs
    df.columns = table_attribs
    df.to_string(index=False)
    #tables = data.find_all('tbody')

    #print(df)
    # rows = tables[2].find_all('tr')
    # for row in rows:
    #     col = row.find_all('td')
    #     if len(col)!=0:
    #         if col[0].find('a') is not None and '—' not in col[2]:
    #             data_dict = {"Name": col[0].a.contents[1],
    #                          "MC_USD_Million": col[2].contents[0][:-1]}
    #             df1 = pd.DataFrame(data_dict, index=[0])
    #             df = pd.concat([df,df1], ignore_index=True)
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exchange_df = pd.read_csv(csv_path)

    # convert first column as keys and second column as values
    exchange_rate = dict(zip(exchange_df.iloc[:, 0], exchange_df.iloc[:, 1]))
    df["MC_GBP_Billion"] = (df["MC_USD_Billion"] * exchange_rate["GBP"]).round(2)
    df["MC_EUR_Billion"] = (df["MC_USD_Billion"] * exchange_rate["EUR"]).round(2)
    df["MC_INR_Billion"] = (df["MC_USD_Billion"] * exchange_rate["INR"]).round(2)

    print(df)
    
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    # Save CSV without index
    df.to_csv(output_path, index=False)


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
table_name = 'Largest_banks'
output_path = './Largest_banks_data.csv'
csv_path='./exchange_rate.csv'

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df,csv_path)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, output_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('Banks.db')

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')

run_query("SELECT * FROM Largest_banks", sql_connection)
run_query("SELECT AVG(MC_GBP_Billion) FROM Largest_banks", sql_connection)
run_query("SELECT Name from Largest_banks LIMIT 5", sql_connection)

log_progress('Process Complete.')

sql_connection.close()

# log_progress('Server Connection closed')