import ibis
import ibis.selectors as s
from ibis import _
import polars as pl

df = ibis.read_csv("data/raw/minimal/INSTRUCTOR.csv",
    column_names = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE'])

#Return only FNAME
(df.to_polars()
    .select(["FNAME"])
)

ibis.to_sql(
    (df.select(["FNAME"]))
)

#Return rows
(df.to_polars()
    .select(pl.len())
)

ibis.to_sql(
    (df.count())
)

#Append
df_row = pl.DataFrame(
    {'ID': [100],
    'FNAME': ['John'],
    'LNAME': ['Doe'],
    'CITY': ['Paris'],
    'CCODE': ['FR']})

df.union_all(df_row)

# 
import ibis

# Turn off (False) to make evaluations lazier
ibis.options.interactive = True

conn = ibis.connect("polars://")

df_raw = conn.read_csv("data/raw/minimal/INSTRUCTOR.csv",
    new_columns = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE'])
df_raw.count()

df_raw.

type(df_raw)


conn.disconnect()
