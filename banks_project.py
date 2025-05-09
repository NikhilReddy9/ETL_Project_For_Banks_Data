import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
import sqlite3
from datetime import datetime
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs_initial = ['Name', 'GDP_USD_Billion']
table_attribs_final = ['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
db_name = 'banks.db'
table_name = 'Largest_banks'
output_csv_path = './Largest_banks_data.csv'
rate_csv_path = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'


# Extract function
def extract(url, table_attribs_initial):
    page = requests.get(url, timeout=60).text
    data = BeautifulSoup(page, 'html.parser')
    tables = data.find_all('table')
    target_table = tables[0]
    df = pd.DataFrame(columns=["Name", "GDP_USD_Billion"])
    rows = target_table.find_all('tr')
    for row in rows[1:]:
        col = row.find_all('td')
        if len(col) >= 3:
            Name = col[1].text.strip()
            GDP_USD_Billion = float(col[2].text.strip().replace('\n', '').replace(',', ''))
            new_data = pd.DataFrame({"Name": [Name], "GDP_USD_Billion": [GDP_USD_Billion]})
            df = pd.concat([df, new_data], ignore_index=True, sort=False)
    df.columns = df.columns.str.strip()
    return df


# Transform function
def transform(df):
    # Read exchange rate CSV file
    exchange_rate = pd.read_csv(rate_csv_path).set_index('Currency').to_dict()['Rate']

    # Add columns for MC_GBP_Billion, MC_EUR_Billion, and MC_INR_Billion
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['GDP_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['GDP_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['GDP_USD_Billion']]

    # print(df[['MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']])
    return df


def load_to_csv(df, output_csv_path):
    df.to_csv(output_csv_path, index=False)


def load_to_db(df, connection, table_name):
    # Create a cursor object to execute SQL queries
    cursor = connection.cursor()
    # Create the table if it doesn't exist
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            Name TEXT,
            GDP_USD_Billion REAL,
            MC_GBP_Billion REAL,
            MC_EUR_Billion REAL,
            MC_INR_Billion REAL
        )
    ''')
    # Insert data into the table
    df.to_sql(table_name, connection, if_exists='replace', index=False)
    # Commit the changes and close the cursor
    connection.commit()
    cursor.close()


def run_queries(query, connection):
    """
    Execute SQL queries and print the query statement along with the output.

    Parameters:
    - query (str): SQL query statement
    - connection (sqlite3.Connection): SQLite database connection
    """
    # Create a cursor object to execute SQL queries
    cursor = connection.cursor()

    # Execute the query
    cursor.execute(query)

    # Fetch the query result
    result = cursor.fetchall()

    # Print the query statement and the result
    print(f"Query Statement: {query}")
    print("Query Result:")
    for row in result:
        print(row)

    # Close the cursor
    cursor.close()


# Log function
def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("./code_log.txt", "a") as f:
        f.write(timestamp + ' : ' + message + '\n')


log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs_initial)
# print(df)

log_progress('Data extraction complete. Initiating Transformation process')

# Assume you have a transform function defined
df = transform(df)

log_progress('Data transformation complete. Initiating loading process')

# Assume you have a load_to_csv function defined
load_to_csv(df, output_csv_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('banks.db')

log_progress('SQL Connection initiated.')

# Assume you have load_to_db and run_query functions defined
load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')

# Query 1: Print the contents of the entire table
query_1 = "SELECT * FROM Largest_banks"
run_queries(query_1, sql_connection)

# Query 2: Print the average market capitalization of all the banks in Billion USD
query_2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_queries(query_2, sql_connection)

# Query 3: Print only the names of the top 5 banks
query_3 = "SELECT Name FROM Largest_banks LIMIT 5"
run_queries(query_3, sql_connection)
# run_query(query_statement, sql_connection)

log_progress('Process Complete.')

sql_connection.close()
