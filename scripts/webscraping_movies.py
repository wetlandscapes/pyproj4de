import requests
# import sqlite3
# import pandas as pd
import polars as pl
from bs4 import BeautifulSoup

# Options
url = 'https://web.archive.org/web/20230902185655/' + \
    'https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
db_name = 'sqlite:///data/processed/Movies.db'
table_name = 'Top_50'
csv_path = 'data/processed/top_50_films.csv'

# Read in data
html_page = requests.get(url).text
data = BeautifulSoup(html_page, 'html.parser')
tables = data.find_all('tbody')
table = tables[0]

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
    df = pl.DataFrame(rows, schema = headers, orient = "row")
    return df

rankings = html_table_to_polars(table)

df = (rankings
    .select(['Average Rank', 'Film', 'Year'])
    .with_columns([
        pl.col('Year')
            .replace("unranked", None)
            .cast(pl.Int64),
        pl.col('Average Rank').cast(pl.Int64)
    ])
    .filter(pl.col('Average Rank') <= 50)
)

#Write to csv
df.write_csv(csv_path)

#Write to database
df.write_database(
    table_name = table_name,
    connection = db_name,
    if_table_exists = "replace"
)

