import duckdb

con = duckdb.connect('/Users/yifanlin/Desktop/MSIN/15799-query/tpc-ds/tpc-ds.db')

# fetch all user tables
tables = con.execute("""
    SELECT table_name
    FROM duckdb_tables()
    WHERE internal = FALSE AND temporary = FALSE
""").fetchall()

print(tables)

# sample & export each
for (tbl,) in tables:
    # turn "main.customers" → "main_customers_sample.csv"
    out = f'/Users/yifanlin/Desktop/MSIN/15799-query/tpc-ds/samples/{tbl}_sample.csv'
    con.execute(f"""
        COPY (
          SELECT * 
          FROM {tbl}
          ORDER BY RANDOM() 
          LIMIT 30000
        ) TO '{out}' (HEADER, DELIMITER ',');
    """)
    print(f'Wrote sample for {tbl} → {out}')