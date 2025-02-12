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

data_url = 'https://web.archive.org/web/20230902185326/' \
    + 'https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)'
tbl_cols = ['Country', 'GDP']
log_file = "logs/etl_project_log.txt"
csv_file = "data/processed/Countries_by_GDP.csv"
db_file = "data/processed/World_Economies.db"
tbl_name = "Countries_by_GDP"

tbl_headers = [
    tbl_cols[0], 'Region',
    'IMF Estimate', 'IMF Year',
    'WB Estimate', 'WB Year',
    'UN Estimate', 'UN Year'
    ]

sql_query = """
    SELECT
    *
    FROM "Countries_by_GDP" AS "t0"
    WHERE
    "t0"."GDP" > 100
"""

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

def url_to_html_table(url: str, index: int) -> Tag:
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    tables = data.find_all('tbody')
    table = tables[index]
    return table

def extract(url: str, index: int, headers: list) -> pl.DataFrame :
    table = url_to_html_table(url, index)
    df = html_table_to_polars(table, headers)
    return df

def transform(df: pl.DataFrame) -> pl.DataFrame:
    out = (
        df
        .select(['Country', 'IMF Estimate'])
        .rename({'IMF Estimate': tbl_cols[1]})
        .with_columns
        (
            pl.col('GDP')
            .str.replace_all(",", "")
            .cast(pl.Int64),
        )
        .with_columns
        (
            (
                pl.col('GDP') / 1000
            )
            .round(2)
        )
    )
    return out

def load_to_csv(df: pl.DataFrame, csv_path) -> None:
    df.write_csv(csv_path)

def load_to_db(
    df: pl.DataFrame,
    sql_connection: ibis.backends.sqlite.Backend,
    table_name: str) -> None:
    sql_connection.create_table(table_name, df)

def run_query(
    query_statement: str,
    sql_connection: Backend) -> None:    
    out = sql_connection.sql(query_statement)
    print(out.execute())

def log_progress(message, log_file = log_file):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file, "a") as f: 
        f.write(timestamp + ',' + message + '\n')

log_progress("ETL job started")
log_progress("Extract phase started")
data_extracted = extract(data_url, 2, tbl_headers)
log_progress("Extract phase finished")

log_progress("Transform phase started")
data_transformed = transform(data_extracted)
log_progress("Transform phase finished") 

log_progress("Load phase started")
log_progress("Loading to csv")
load_to_csv(data_transformed, csv_file)
log_progress("Done loading to csv")

log_progress("Loading data to sqlite database")
conn = ibis.sqlite.connect(db_file)

load_to_db(
    df = data_transformed,
    sql_connection = conn,
    table_name = tbl_name
    )
log_progress("Done loading to sqlite database")
log_progress("Load phase finished")

log_progress("Running query on databse")
run_query(sql_query, conn)
log_progress("Finished runnning query on databse")

conn.disconnect()
log_progress("ETL job finished\n")
