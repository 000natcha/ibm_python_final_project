import requests
from bs4 import BeautifulSoup
import pandas as pd 
import sqlite3
import numpy as np 
from datetime import datetime

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ":" + message + "\n")
    return

def extract(url, table_attribs):
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, "html.parser")

    df = pd.DataFrame(columns=table_attribs)
    
    tables = data.find_all("tbody")
    rows = tables[0].find_all("tr")

    for row in rows:
        col = row.find_all("td")
        if len(col) != 0:
            bank_name = str(col[1].find_all("a")[1]["title"])
            market_cap = float(col[2].contents[0][:-1])
            data_dict = {"Name": bank_name, 
                    "MC_USD_Billion": market_cap}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    return df

def transform(df, csv_path):
    exchange_df = pd.read_csv(csv_path)
    exchange_rate = exchange_df.set_index('Currency').to_dict()['Rate']

    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]
    return df 

def load_to_csv(df, output_path):
    df.to_csv(output_path)
    return

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists="replace", index=False)
    return

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
    return

url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ["Name", "MC_USD_Billion"]
csv_path = "exchange_rate.csv"
output_path = "Largest_banks_data.csv"
db_name = "Banks.db"
table_name = "Largest_banks"
log_file = "code_log.txt"

log_progress("Preliminaries complete. Initiating ETL process")
df = extract(url, table_attribs)
log_progress("Data extraction complete. Initiating Transformation process")
df = transform(df, csv_path)
log_progress("Data transformation complete. Initiating Loading process")
load_to_csv(df, output_path)
log_progress("Data saved to CSV file")

sql_connection = sqlite3.connect(db_name)
log_progress("SQL Connection initiated")
load_to_db(df, sql_connection, table_name)
log_progress("Data loaded to Database as a table, Executing queries")

query_statements = ["SELECT * FROM Largest_banks",
                    "SELECT AVG(MC_GBP_Billion) FROM Largest_banks",
                    "SELECT Name from Largest_banks LIMIT 5"]
for statement in query_statements:
    run_query(statement, sql_connection)
log_progress("Process Complete")

sql_connection.close()
log_progress("Server Connection closed")