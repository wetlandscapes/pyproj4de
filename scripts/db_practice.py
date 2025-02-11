import ibis
import polars as pl

db_file = 'data/processed/STAFF.db'
tbl_nm = 'Departments'
data_file = 'data/raw/minimal/Departments.csv'

data_raw = pl.read_csv(
    data_file,
    new_columns = [
        'DEPT_ID',
        'DEP_NAME',
        'MANAGER_ID',
        'LOC_ID'
    ]
)

# Tried inserting just a dict, but got index error; easier to use dataframe
new_row = pl.DataFrame({
    'DEPT_ID': 9,
    'DEP_NAME': 'Quality Assurance',
    'MANAGER_ID': 30010,
    'LOC_ID': 'L0010'
})

conn = ibis.sqlite.connect(db_file)

conn.create_table(tbl_nm, data_raw)

conn.table(tbl_nm).execute()

conn.insert(tbl_nm, new_row)

(
    conn
    .table(tbl_nm)
    .execute()
)

(
    conn
    .table(tbl_nm)
    .select('DEP_NAME')
    .execute()
)

(
    conn
    .table(tbl_nm)
    .count()
    .execute()
)
