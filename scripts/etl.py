import polars as pl
from datetime import datetime
from src import pyproj4de as de

data_dir = "data/raw/minimal"
data_file = "data/processed/source.csv"
log_file = "logs/log_file.txt"

def log_progress(message, log_file = log_file):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n')

log_progress("ETL job started")

log_progress("Extract phase started")
extracted_data = de.extract_data(data_dir)
log_progress("Extract phase finished")

log_progress("Transform phase started") 
type_schema = {
    'height': pl.Float64,
    'weight': pl.Float64
    }

typed_data = de.transform_type(extracted_data, schema = type_schema)

unit_schema = {
    'height': ('inch', 'meter'),
    'weight': ('pound', 'kilogram')
    }

converted_data = de.transform_unit(typed_data, conversions = unit_schema)
log_progress("Transform phase finished") 

log_progress("Load phase started") 
de.write_data(data_file, data = converted_data)
log_progress("Load phase finished") 

log_progress("ETL job finished")