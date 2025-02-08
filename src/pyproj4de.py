import os
import glob
import polars as pl
import xml.etree.ElementTree as ET
from datetime import datetime


def extract_csv(file, **kwargs):
    df = pl.read_csv(file, **kwargs)
    return df


def extract_json(file, **kwargs):
    df = pl.read_ndjson(file, **kwargs)
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

    data = []
    
    for file in glob.glob(os.path.join(dir, "*.csv")):
        data_csv = extract_csv(file, columns = columns)
        data.append(data_csv)

    for file in glob.glob(os.path.join(dir, "*.json")):
        data_json = extract_json(file)
        data.append(data_json)

    # for file in glob.glob(os.path.join(dir, "*.xml")):
    #     data_xml = extract_xml(file, columns)
    #     data.append(data_xml)

    df = pl.concat(data)

    return (df)


extract_json("data/raw/minimal/source1.json")

tmp = extract("data/raw/minimal", columns = ['name', 'height', "weight"])
