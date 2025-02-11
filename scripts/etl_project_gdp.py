import requests
import polars as pl
from bs4 import BeautifulSoup

data_url = 'https://web.archive.org/web/20230902185326/' \
    + 'https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)'
log_file = "logs/etl_project_log.txt"
csv_file = "data/raw/minimal/Countries_by_GDP.csv"
db_file = "data/processed/World_Economies.db"
tbl_name = "Countries_by_GDP"
tbl_cols = ['Country', 'GDP']

# Read in data


def html_table_to_polars(table):
    # Extract headers
    headers = []
    for th in table.find_all('th'):
        headers.append(th.text.strip())
    
    # If no headers found, use empty strings
    if not headers:
        headers = [f'column_{i}' for i in range(len(table.find('tr').find_all(['td', 'th'])))]
    
    # Extract rows
    rows = []
    for tr in table.find_all('tr'):
        row = []
        for td in tr.find_all(['td', 'th']):
            row.append(td.text.strip())
        if row:  # Skip empty rows
            rows.append(row)
    
    # Remove header row if it's in the rows data
    if headers:
        rows = [row for row in rows if row != headers]
    
    # Create Polars DataFrame
    # print(rows)
    df = pl.DataFrame(rows, schema = headers, orient = "row")
    return df

def url_to_df(url, index) :
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    tables = data.find_all('tbody')
    table = tables[index]
    df = html_table_to_polars(table)

    return df

(requests
    .get(data_url)
    .text)

tmp_html_page = requests.get(data_url).text
tmp_data = BeautifulSoup(tmp_html_page, 'html.parser')
tmp_tables = tmp_data.find_all('tbody')

tmp_tables[0]

tmp_table = tmp_tables.find_all()

# help(tmp_tables[2].decompose)

tmp_data = html_table_to_polars(tmp_tables[2])

data_raw = url_to_df(data_url, 2)

data_raw
