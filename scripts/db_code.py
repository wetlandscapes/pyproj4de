import ibis
import polars as pl

tbl_nm = 'INSTRUCTOR'

# Turn off (False) to make evaluations lazier
ibis.options.interactive = True

# Establish connection
conn = ibis.sqlite.connect("data/processed/STAFF.db")

# Load data
data_raw = pl.read_csv("data/raw/minimal/INSTRUCTOR.csv",
    new_columns = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE'])

# Return only FNAME column
data_raw.select("FNAME")

# Get info about object
# help(data_raw)

# Checkout the number of entries (rows)
data_raw.select(pl.len())

# Create table
conn.create_table(tbl_nm, data_raw)

# New row to insert into table
df_row = pl.DataFrame(
    {'ID': [100],
    'FNAME': ['John'],
    'LNAME': ['Doe'],
    'CITY': ['Paris'],
    'CCODE': ['FR']})

# Check that the schema matches
data_raw.vstack(df_row)

# Add entry to database
conn.insert(tbl_nm, df_row)

# Check that the data looks right
conn.table(tbl_nm).to_polars()

# Show SQL
print(
    ibis.to_sql(
        conn.table(tbl_nm)
    )
)

# Just FNAME column
(
    conn
    .table(tbl_nm)
    .select('FNAME')
)

# Number of rows
(
    conn
    .table(tbl_nm)
    .count()
)


conn.disconnect()
