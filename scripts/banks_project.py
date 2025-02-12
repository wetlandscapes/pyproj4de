# This allows pyproj4de dependency to be run interactively or from terminal
if __name__ == "__main__":
    import sys
    from pathlib import Path
    sys.path.append(str(Path(__file__).parent.parent))

from datetime import datetime
import requests
import polars as pl
from bs4 import BeautifulSoup, Tag
import ibis
from ibis.backends.sqlite import Backend

def log_progress(message: str, log_file: str) -> None:
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format) 
    with open(log_file, "a") as f: 
        f.write(timestamp + ': ' + message + '\n')

def url_to_html_table(url: str, index: int) -> Tag:
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    tables = data.find_all('tbody')
    table = tables[index]
    return table

def html_table_to_polars(table: Tag, headers: list) -> pl.DataFrame:
    # Extract rows
    rows = []
    for tr in table.find_all('tr'):
        # Skip header rows
        if 'static-row-header' in tr.get('class', []):
            continue

        row = []
        for td in tr.find_all(['td']):
            # Get text content and clean it
            content = td.text.strip()
            row.append(content)
    
        if row and len(row) == len(headers):  # Only add rows that match header length
            rows.append(row)
    
    # Create Polars DataFrame
    df = pl.DataFrame(rows, schema = headers, orient = "row")
    return df

def extract(url: str, index: int, headers: list) -> pl.DataFrame :
    table = url_to_html_table(url, index)
    df = html_table_to_polars(table, headers)
    return df

def data_transform(data: pl.DataFrame, col_ignore: list) -> pl.DataFrame:
    df = (
        data
        .select(col_ignore)
        .with_columns(
            pl.col(col_ignore[1])
            .cast(pl.Float64)
        )
    )
    return df

def currency_transform(currency: pl.DataFrame) -> pl.DataFrame:
    df = (
        currency
        .with_columns(
            (pl.lit("MC_") + pl.col("Currency") + pl.lit("_Billion"))
                .alias("Currency"),
            pl.lit(1).alias("idx")
        )
        .pivot(
            on = "Currency",
            index = "idx",
            values = "Rate"
        )
        .drop("idx")
    )
    return df

def data_combine(data, currency, col_ignore):
    df = (
        pl.concat([data, currency], how = "horizontal")
        .with_columns(
            pl.exclude(col_ignore)
            .fill_null(strategy = "forward")
        )
        .with_columns(
            pl.exclude(col_ignore)
            .mul(col_ignore[1])
            .round(2)
        )
    )
    return df


def transform(data: pl.DataFrame,
    currency: pl.dataframe,
    col_ignore: list) -> pl.DataFrame:
    dat = data_transform(data, col_ignore)
    cur = currency_transform(currency)
    df = data_combine(dat, cur, col_ignore)
    return df

def load_to_csv(df: pl.DataFrame, csv_path) -> None:
    df.write_csv(csv_path)

def load_to_db(
    df: pl.DataFrame,
    sql_connection: ibis.backends.sqlite.Backend,
    table_name: str) -> None:
    sql_connection.create_table(table_name, df, overwrite = True)

def run_query(
    query_statement: str,
    sql_connection: Backend) -> None:    
    out = sql_connection.sql(query_statement)
    print(out.execute())

# --- Entities ---
data_url = 'https://web.archive.org/web/20230908091635/' \
    + 'http://en.wikipedia.org/wiki/List_of_largest_banks'
tbl_pos = 0
exchange_csv_file = 'https://cf-courses-data.s3.us.cloud-object-storage' \
    + '.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2' \
    + '/exchange_rate.csv'
tbl_cols = ['Name', 'MC_USD_Billion']
# trans_cols = tbl_cols + ['MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
log_file = "logs/code_log.txt"
load_csv_file = "data/processed/Largest_banks_data.csv"
db_file = "data/processed/Banks.db"
tbl_name = "Largest_banks"
tbl_headers = ['Rank'] + tbl_cols
sql_query_1 = """
    SELECT * FROM Largest_banks
"""
sql_query_2 = """
    SELECT AVG(MC_GBP_Billion) FROM Largest_banks
"""
sql_query_3 = """
    SELECT Name FROM Largest_banks LIMIT 5
"""

# --- Call functions ---
log_progress("Preliminaries complete. Initiating ETL process", log_file)

data_extracted = extract(data_url, tbl_pos, tbl_headers)
exchange_data = pl.read_csv(exchange_csv_file)
log_progress("Data extraction complete. Initiating Transformation process", log_file)

data_transformed = transform(data_extracted, exchange_data, tbl_cols)
log_progress("Data transformation complete. Initiating Loading process", log_file) 

load_to_csv(data_transformed, load_csv_file)
log_progress("Data saved to CSV file", log_file)

conn = ibis.sqlite.connect(db_file)
log_progress("SQL Connection initiated", log_file)

load_to_db(df = data_transformed, sql_connection = conn, table_name = tbl_name)
log_progress("Data loaded to Database as a table, Executing queries", log_file)

run_query(sql_query_1, conn)
run_query(sql_query_2, conn)
run_query(sql_query_3, conn)
log_progress("Process Complete", log_file)

conn.disconnect()
log_progress("Server Connection closed", log_file)
