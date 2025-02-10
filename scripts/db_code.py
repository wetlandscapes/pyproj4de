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
