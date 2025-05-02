import duckdb
import pandas as pd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# PostgreSQL connection info
PG_CONN_INFO = {
    'host': 'localhost',
    'port': 28814,
    'dbname': 'hypostats'
}


def create_table_and_copy_to_pg(table):
    print(f"Processing: {table}")
    df = con.execute(f"SELECT * FROM {table}").df()

    # Sanitize column names
    df.columns = [c.lower() for c in df.columns]

    # Create table in PostgreSQL
    col_defs = []
    for col_name, dtype in zip(df.columns, df.dtypes):
        if pd.api.types.is_integer_dtype(dtype):
            col_type = 'BIGINT'
        elif pd.api.types.is_float_dtype(dtype):
            col_type = 'DOUBLE PRECISION'
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            col_type = 'TIMESTAMP'
        else:
            col_type = 'TEXT'
        col_defs.append(f'"{col_name}" {col_type}')
    create_stmt = f'CREATE TABLE IF NOT EXISTS "{table}" ({", ".join(col_defs)});'
    pg_cur.execute(create_stmt)

    # Upload using COPY via StringIO buffer
    from io import StringIO
    buf = StringIO()
    df.to_csv(buf, index=False, header=False, sep='\t', na_rep='\\N')
    buf.seek(0)
    pg_cur.copy_from(buf, table, sep='\t', null='\\N', columns=list(df.columns))


if __name__ == '__main__':
    # Connect to DuckDB and load TPC-DS extension
    con = duckdb.connect()
    con.execute("INSTALL tpcds; LOAD tpcds;")
    scale = 1  # you can change this
    con.execute(f'CALL dsdgen(sf = {scale});')

    # TPC-DS base table names (simplified, full list can be longer)
    tpcds_tables = con.execute("show tables").fetchall()
    tpcds_tables = [t[0] for t in tpcds_tables]

    # Connect to PostgreSQL
    pg_con = psycopg2.connect(**PG_CONN_INFO)
    pg_con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    pg_cur = pg_con.cursor()

    for table in tpcds_tables:
        create_table_and_copy_to_pg(table)

    pg_cur.close()
    pg_con.close()
    con.close()
    print("✅ All tables transferred.")
