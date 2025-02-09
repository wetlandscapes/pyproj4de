# This allows pyproj4de dependency to be run interactively or from terminal
if __name__ == "__main__":
    import sys
    from pathlib import Path
    sys.path.append(str(Path(__file__).parent.parent))

import polars as pl
from datetime import datetime

from src import pyproj4de as de

data_dir = "data/raw/minimal/datasource"
data_file = "data/processed/datasource.csv"
log_file = "logs/datasource_log_file.txt"

type_schema = {
    # 'height': pl.Float64,
    # 'weight': pl.Float64
    }

unit_schema = {
    # 'height': ('inch', 'meter'),
    # 'weight': ('pound', 'kilogram')
    }

def log_progress(message, log_file = log_file):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file, "a") as f: 
        f.write(timestamp + ',' + message + '\n')

log_progress("ETL job started")

log_progress("Extract phase started")
extracted_data = de.extract_data(data_dir)
log_progress("Extract phase finished")

log_progress("Transform phase started") 
typed_data = de.transform_type(extracted_data, schema = type_schema)
converted_data = de.transform_unit(typed_data, conversions = unit_schema)
log_progress("Transform phase finished") 

log_progress("Load phase started") 
de.write_data(data_file, data = converted_data)
log_progress("Load phase finished") 

log_progress("ETL job finished\n")