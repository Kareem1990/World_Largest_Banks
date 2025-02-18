# pip install --upgrade pip
# pip install beautifulsoup4 requests pandas numpy
# pip install beautifulsoup4 requests pandas



from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ["Name", "MC_USD_Billion"]
csv_path = './exchange_rate.csv'
output_csv = "./Largest_banks_data.csv"
db_name = "Banks.db"
table_name = "Largest_banks"
sql_connection = sqlite3.connect('Banks.db')


def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''

    timestamp_format = '%Y-%h-%d-%H:%M:%S'# Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    # Write the log entry to code_log.txt
    with open("./code_log.txt", "a") as log_file:
        log_file.write(f"{timestamp} : {message}\n")
    
    # Print log for immediate feedback
    print(f"{timestamp} : {message}")


def extract(url, table_attribs):

    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        if row.find('td') is not None:
            col = row.find_all('td')
            data_dict = {"Name":col[1].text.strip(),
                    "MC_USD_Billion": col[2].text.strip()}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    return df

log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url, table_attribs)
print(df)


def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    #  Step 1: Read exchange rate CSV into a DataFrame
    exchange_df = pd.read_csv(csv_path)

    #  Step 2: Convert exchange rates into a dictionary
    exchange_rate = exchange_df.set_index('Currency').to_dict()['Rate']

    #  Step 3: Convert MC_USD_Billion to float
    df['MC_USD_Billion'] = df['MC_USD_Billion'].str.replace(',', '').astype(float)

    # Step 4: Apply exchange rates and create new columns
    df['MC_GBP_Billion'] = np.round(df['MC_USD_Billion'] * exchange_rate['GBP'], 2)
    df['MC_EUR_Billion'] = np.round(df['MC_USD_Billion'] * exchange_rate['EUR'], 2)
    df['MC_INR_Billion'] = np.round(df['MC_USD_Billion'] * exchange_rate['INR'], 2)

    return df
log_progress('Data extraction complete. Initiating Transformation process')
df = transform(df, csv_path)
print(df['MC_EUR_Billion'][4])
print(df)



def load_to_csv(df, output_csv):
    df.to_csv(output_csv, index=False)
    print(f"Data successfully saved to: {output_csv}")

log_progress('Data transformation complete. Initiating loading process')
load_to_csv(df, output_csv)
log_progress('Data saved to CSV file')

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

log_progress('SQL Connection initiated.')
load_to_db(df,sql_connection, table_name)


def run_query(sql_connection, query_statement):
    ''' This function runs the query on the database table,
    prints the query statement, and returns the results. '''

    cursor = sql_connection.cursor()  # Create a cursor object
    cursor.execute(query_statement)   #  Execute the SQL query
    results = cursor.fetchall()       #  Fetch all results

    print(f"Query: {query_statement}")  # Print the query being executed
    for row in results:
        print(row)  # Print each row from the results

    return results  # Return results if needed

# Run the required queries
query1 = "SELECT * FROM Largest_banks"
query2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
query3 = "SELECT Name FROM Largest_banks LIMIT 5"

log_progress('Data loaded to Database as table. Running the query')
run_query(sql_connection, query1)  # Print full table
run_query(sql_connection, query2)  # Print avg market cap
run_query(sql_connection, query3)  #  Print top 5 bank names

log_progress('Process Complete.')

# Close the SQL connection at the end
sql_connection.close()