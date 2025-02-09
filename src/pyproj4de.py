import os
import glob
import polars as pl
import xml.etree.ElementTree as ET
# from datetime import datetime

def extract_csv(file, columns = None, options = None):
    """Extract data from CSV file.
    
    Args:
        file: Path to CSV file
        columns: List of columns to extract
        options: Dict of options for pl.read_csv
    """
    options = options or {}
    return pl.read_csv(
        file,
        columns = columns,
        infer_schema=False,
        **options
    )

def extract_json(file, columns = None, options = None):
    """Extract data from JSON file.
    
    Args:
        file: Path to JSON file
        columns: List of columns to extract (unused but kept for consistency)
        options: Dict of options for pl.read_ndjson
    """
    options = options or {}
    df = pl.read_ndjson(file, **options)
    
    # Convert all columns to string to be consistent
    df = df.select([
        pl.col(col).cast(pl.String) for col in df.columns
    ])

    return df

def extract_xml(file, columns = None, options = None):
    """Extract data from XML file.
    
    Args:
        file: Path to XML file
        columns: List of columns to extract (optional)
        options: Dict with optional keys:
            - parse: Dict of options for ET.parse
            - df: Dict of options for pl.DataFrame
    """
    options = options or {}
    parse_options = options.get('parse', {})
    df_options = options.get('df', {})
    
    tree = ET.parse(file, **parse_options)
    root = tree.getroot()
    
    # Get all records (assuming consistent structure like in the example)
    records = root.findall('./*')  # Find all immediate children
    
    if not records:
        return pl.DataFrame()
        
    # Get columns from the first record if not provided
    if columns is None:
        columns = [child.tag for child in records[0]]

    data = []
    for record in records:
        row = {}
        for column in columns:
            element = record.find(column)
            row[column] = element.text if element is not None else None
        data.append(row)
    
    return pl.DataFrame(data, **df_options)


def extract(dir_path, columns = None, options = None):
    """Extract data from multiple file types in a directory.
    
    Args:
        dir_path: Path to directory containing files
        columns: List of columns to extract (optional)
        options: Dict with optional keys:
            - csv: Dict of options for CSV extraction
            - json: Dict of options for JSON extraction
            - xml: Dict of options for XML extraction
            
    Example:
        options = {
            'csv': {
                'separator': '|',
                'skip_rows': 1
            },
            'json': {
                'encoding': 'utf-8'
            },
            'xml': {
                'parse': {'encoding': 'utf-8'},
                'df': {'schema': {'col1': pl.String}}
            }
        }
        df = extract('data_directory', columns=['col1', 'col2'], options=options)
    """

    # Get any options passed to sub-routines
    options = options or {}
    
    # Initialize
    data = []

    # Read CSV files
    for file in glob.glob(os.path.join(dir_path, "*.csv")):
        df = extract_csv(
            file,
            columns = columns,
            options = options.get('csv')
        )
        data.append(df)
    
    # Read JSON files
    for file in glob.glob(os.path.join(dir_path, "*.json")):
        df = extract_json(
            file,
            columns = columns,
            options = options.get('json')
        )
        data.append(df)
    
    # Read XML files
    for file in glob.glob(os.path.join(dir_path, "*.xml")):
        df = extract_xml(
            file,
            columns = columns,
            options = options.get('xml')
        )
        data.append(df)
    
    # Return combined data or empty data frame if no data
    return pl.concat(data) if data else pl.DataFrame()
