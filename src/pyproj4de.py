import os
import glob
import polars as pl
import xml.etree.ElementTree as ET
# from datetime import datetime


def extract_csv(file, **kwargs):
    # Schema method ensures consistency (all strings)
    df = pl.read_csv(file, infer_schema = False, **kwargs)
    return df


def extract_json(file, **kwargs):
    df = pl.read_ndjson(file, **kwargs)

    # Convert all columns to string to be consistent
    df = df.select([
        pl.col(col).cast(pl.String) for col in df.columns
    ])

    return df


def extract_xml(file, columns, kwargs=None):
    kwargs = kwargs or {}  # Default to empty dict if None
    parse_kwargs = kwargs.get('parse', {})
    df_kwargs = kwargs.get('df', {})

    tree = ET.parse(file, **parse_kwargs)
    roots = tree.getroot()

    data = []

    for root in roots:
        row = {}
        for column in columns:
            value = root.find(column).text
            row[column] = value
        data.append(row)
    
    df = pl.DataFrame(data, **df_kwargs)
    
    return df


def extract(dir, columns, **kwargs):

    # Initialize
    data = []
    
    # Read in csv files
    for file in glob.glob(os.path.join(dir, "*.csv")):
        data_csv = extract_csv(file, columns = columns)
        data.append(data_csv)

    # Read in json files
    for file in glob.glob(os.path.join(dir, "*.json")):
        data_json = extract_json(file)
        data.append(data_json)

    # Read in xml files
    for file in glob.glob(os.path.join(dir, "*.xml")):
        data_xml = extract_xml(file, columns)
        data.append(data_xml)

    # Combine all the data -- all columns will be string
    df = pl.concat(data)

    return (df)
