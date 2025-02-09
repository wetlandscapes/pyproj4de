import os
import glob
import pint
import polars as pl
import xml.etree.ElementTree as ET

# Initialize pint registry
ureg = pint.UnitRegistry()

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


def extract_data(dir_path, columns = None, options = None):
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

def transform_type(data, schema = None):
    """Transform DataFrame by converting types of schema-defined columns.
    
    Args:
        df: polars.DataFrame to transform
        schema: Optional dict defining columns and their data types
                Keys are column names and values are polars data types
                Example: {'price': pl.Float32, 'quantity': pl.Int32}
                If None, returns DataFrame unchanged
    
    Returns:
        polars.DataFrame with schema-defined columns converted and all other columns preserved
        
    Example:
        schema = {
            'price': pl.Float32,      # Convert to 32-bit float
            'quantity': pl.Int32,     # Convert to 32-bit integer
        }
        # All columns are preserved, but 'price' and 'quantity' are converted
        df_transformed = transform(df, schema)
        
        # Without schema, returns DataFrame unchanged
        df_unchanged = transform(df)
    """
    
    if schema is None:
        return data

    # Loop through schema-defined columns, changing type
    expressions = [
        pl.col(col).cast(
            schema[col],
            strict=False) if col in schema else pl.col(col)
        for col in data.columns
    ]

    return data.select(expressions)

def apply_conversion(column_name, from_unit, to_unit):
    """Create a polars expression to convert a column from one unit to another.
    
    Args:
        column_name: Name of the column to convert
        from_unit: Source unit (e.g., 'gram', 'cm', 'celsius')
        to_unit: Target unit (e.g., 'pound', 'inch', 'fahrenheit')
    
    Returns:
        polars.Expr: Expression that can be used in select or with_columns
        
    Example:
        # Single column conversion
        df.select([
            apply_conversion('weight', 'gram', 'pound'),
            pl.col('*')
        ])
        
        # Multiple conversions
        df.with_columns([
            apply_conversion('weight', 'gram', 'pound'),
            apply_conversion('height', 'cm', 'inch')
        ])
    """
    factor = ureg(from_unit).to(to_unit).magnitude
    return pl.col(column_name) * factor

def transform_unit(df, conversions = None):
    """Convert units for specified columns in DataFrame.
    
    Args:
        df: polars.DataFrame to transform
        unit_map: Optional dict mapping column names to unit conversion tuples
                 Each tuple should be (from_unit, to_unit)
                 Example: {'weight': ('gram', 'pound')}
    
    Returns:
        polars.DataFrame with unit conversions applied and other columns preserved

    Example:
        df = pl.DataFrame({'height': [64.0, 72.0], 'weight': [100.0, 200.0]})
        schema = {
            'height': ('inch', 'meter'),
            'weight': ('pound', 'kilogram')
        transform_unit(df, conversions = schema)
    }
    """
    if conversions is None:
        return df
    
    expressions = [
        apply_conversion(col, *conversions[col]) if col in conversions else pl.col(col)
        for col in df.columns
    ]
    
    return df.select(expressions)

def write_data(file, data):
    data.write_csv(file)
    """Write transformed data to a csv file.

    Args:
        file: To location of the file to be written, appended with ".csv"
        data: The dataframe (polars.DataFrame) to be written to disk

    Returns:
        None

    Example:
        write_data("my_data.csv", polars_df)
    """