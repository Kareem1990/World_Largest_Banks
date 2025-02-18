# ✅ Required Library Imports
from bs4 import BeautifulSoup  # Web scraping: Extract HTML table data
import requests  # HTTP requests: Fetch the webpage content
import pandas as pd  # Data manipulation
import numpy as np  # Numerical operations: Used for rounding values
import sqlite3  # SQLite database: Store extracted data
from datetime import datetime  # Logging timestamps

# ✅ Global Variables
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ["Name", "MC_USD_Billion"]  # Columns for extracted table
csv_path = './exchange_rate.csv'  # Path for exchange rate CSV file
output_csv = "./Largest_banks_data.csv"  # Output CSV file
db_name = "Banks.db"  # SQLite database name
table_name = "Largest_banks"  # Database table name
sql_connection = sqlite3.connect(db_name)  # Establish SQLite connection


# ✅ Logging Function: Writes execution logs to a text file
def log_progress(message):
    '''Logs the mentioned message with a timestamp.'''
    timestamp_format = '%Y-%b-%d-%H:%M:%S'  # Example format: 2025-Feb-17-19:43:55
    now = datetime.now()  
    timestamp = now.strftime(timestamp_format)  
    with open("./code_log.txt", "a") as log_file:  # Open log file in append mode
        log_file.write(f"{timestamp} : {message}\n")  
    print(f"{timestamp} : {message}")  # Immediate feedback


# ✅ Extract Function: Scrapes the webpage and extracts bank data
def extract(url, table_attribs):
    '''Fetches the webpage, extracts the bank data, and returns a DataFrame.'''
    
    page = requests.get(url).text  # Fetch webpage content as text
    data = BeautifulSoup(page, 'html.parser')  # Parse HTML with BeautifulSoup
    df = pd.DataFrame(columns=table_attribs)  # Initialize empty DataFrame

    tables = data.find_all('tbody')  # Find all table body elements
    rows = tables[1].find_all('tr')  # Get all rows from the second table (banks)

    for row in rows:
        col = row.find_all('td')  # Extract all columns (cells) from each row
        if len(col) >= 3:  # Ensure there are at least 3 columns to avoid errors
            data_dict = {
                "Name": col[1].text.strip(),  # Extract bank name, remove whitespace
                "MC_USD_Billion": col[2].text.strip()  # Extract market cap
            }
            df1 = pd.DataFrame(data_dict, index=[0])  # Convert dict to DataFrame
            df = pd.concat([df, df1], ignore_index=True)  # Append row to DataFrame

    return df


# ✅ Initiate Extraction Process
log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url, table_attribs)
print(df)  # Display extracted data


# ✅ Transform Function: Converts currency and adds new columns
def transform(df, csv_path):
    '''Converts Market Cap to different currencies and returns updated DataFrame.'''
    
    exchange_df = pd.read_csv(csv_path)  # Load exchange rates from CSV
    exchange_rate = exchange_df.set_index('Currency').to_dict()['Rate']  
    # ✅ Converts to dictionary { 'GBP': 0.76, 'EUR': 0.93, 'INR': 83.0 }
    # ✅ Why? So we can easily retrieve exchange rates using currency keys
    
    df['MC_USD_Billion'] = df['MC_USD_Billion'].str.replace(',', '').astype(float)  
    # ✅ Converts "5,742.86" → 5742.86 (removes commas) → Converts to float
    
    # ✅ Convert Market Cap from USD to other currencies & round to 2 decimals
    df['MC_GBP_Billion'] = np.round(df['MC_USD_Billion'] * exchange_rate['GBP'], 2)
    df['MC_EUR_Billion'] = np.round(df['MC_USD_Billion'] * exchange_rate['EUR'], 2)
    df['MC_INR_Billion'] = np.round(df['MC_USD_Billion'] * exchange_rate['INR'], 2)
    # ✅ Why use np.round(..., 2)? To limit decimal places to 2 for financial data

    return df


# ✅ Initiate Transformation Process
log_progress('Data extraction complete. Initiating Transformation process')
df = transform(df, csv_path)
print(df['MC_EUR_Billion'][4])  # Print Market Cap of 5th largest bank in EUR
print(df)  # Display transformed DataFrame


# ✅ Save Data to CSV
def load_to_csv(df, output_csv):
    '''Saves the transformed DataFrame to a CSV file.'''
    df.to_csv(output_csv, index=False)  # Save without index column
    print(f"Data successfully saved to: {output_csv}")


log_progress('Data transformation complete. Initiating loading process')
load_to_csv(df, output_csv)
log_progress('Data saved to CSV file')


# ✅ Load Data to SQLite Database
def load_to_db(df, sql_connection, table_name):
    '''Stores the DataFrame into an SQLite database.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)  
    # ✅ Why 'replace'? Ensures table updates with new data


log_progress('SQL Connection initiated.')
load_to_db(df, sql_connection, table_name)


# ✅ Query Function: Executes SQL queries on the database
def run_query(sql_connection, query_statement):
    '''Runs an SQL query and prints the result.'''
    
    cursor = sql_connection.cursor()  # Create a cursor object for database interaction
    cursor.execute(query_statement)  # Execute the query
    results = cursor.fetchall()  # Fetch all results

    print(f"Query: {query_statement}")  # Print executed query
    for row in results:
        print(row)  # Print each row in the result

    return results  # Return results (if needed)


# ✅ Run SQL Queries & Print Results
query1 = "SELECT * FROM Largest_banks"  # Retrieve full table
query2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"  # Compute average market cap in GBP
query3 = "SELECT Name FROM Largest_banks LIMIT 5"  # Get names of top 5 banks

log_progress('Data loaded to Database as table. Running the query')
run_query(sql_connection, query1)  # Print full table
run_query(sql_connection, query2)  # Print avg market cap
run_query(sql_connection, query3)  # Print top 5 bank names

log_progress('Process Complete.')

# # ✅ Close SQL Connection at the End
# sql_connection.close()  
# # ✅ Why close the connection? To free system resources and prevent locking issues
# 🔹 Summary of Key Fixes & Explanations
# ✅ Why convert the DataFrame to a dictionary?

# exchange_rate = exchange_df.set_index('Currency').to_dict()['Rate']
# Reason: Makes exchange rates easy to access via currency keys (e.g., 'GBP' → 0.76).
# ✅ Why remove commas & convert Market Cap to float?

# "5,742.86" → "5742.86" → float(5742.86)
# Reason: Strings with commas can't be used in mathematical operations.
# ✅ Why use np.round(..., 2)?

# Ensures financial data is formatted correctly by rounding to 2 decimal places.
# ✅ Why if_exists='replace' in to_sql()?

# Replaces existing database table each time the script runs, preventing duplicate data.
# ✅ Why close the SQL connection at the end?

# Prevents database locking issues and ensures efficient resource usage.
