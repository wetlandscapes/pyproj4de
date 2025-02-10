import ibis

con = ibis.connect("polars://")

df_raw = con.read_csv("data/raw/minimal/INSTRUCTOR.csv",
    new_columns = ['id', 'FNAME', 'LNAME', 'x', 'code'])
df_raw
df_raw.execute()

