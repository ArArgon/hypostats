import json
import pandas as pd


QUERY_DIR = '/Users/yifanlin/Desktop/MSIN/15799-query/tpc-ds/queries'
SAMPLES_DIR = '/Users/yifanlin/Desktop/MSIN/15799-query/tpc-ds/samples'
RESULTS_DIR = '/Users/yifanlin/Desktop/MSIN/15799-query/hypostats/python/results'
with open('/Users/yifanlin/Desktop/MSIN/15799-query/hypostats/python/table_to_query.json', 'r') as f:
    TABLE_TO_QUERY = json.load(f)
KL_PD = pd.read_csv('/Users/yifanlin/Desktop/MSIN/15799-query/hypostats/python/kl_list.csv')
KL_LIST = KL_PD.values.tolist()
TARGET_TABLES = ['customer', 'catalog_sales']
PREFIX = {'customer': 'c_', 'catalog_sales': 'cs_'}

TIMEOUT_MS = 10 * 60 * 1000
N_SPLIT = 6


import psycopg2
import os
import traceback
from io import StringIO


conn = psycopg2.connect(dbname='tpc_ds')


def explain_analyze(table: str, query_file: str):
    print(f'Running EXPLAIN ANALYZE on table {table}, query {query_file}')
    with open(os.path.join(QUERY_DIR, query_file)) as f:
        query = f.read()
    query = query.replace('`', '"')
    query = 'EXPLAIN (ANALYZE, FORMAT TEXT) ' + query
    os.makedirs(os.path.join(RESULTS_DIR, table), exist_ok=True)
    output_file = os.path.join(RESULTS_DIR, table, query_file)

    cursor = conn.cursor()
    try:
        cursor.execute(f"SET statement_timeout = {TIMEOUT_MS};")
        cursor.execute(query)
        conn.commit()
        results = cursor.fetchall()
        with open(output_file, 'w') as f:
            for row in results:
                f.write(f"{row[0]}\n")
    except psycopg2.extensions.QueryCanceledError:
        conn.rollback()
        print(f'Query {query_file} timed out after {TIMEOUT_MS} ms')
        with open(output_file, 'w') as f:
            f.write(f'Query {query_file} timed out after {TIMEOUT_MS} ms\n')
    except Exception as e:
        conn.rollback()
        print(f'Query {query_file} fails')
        string_buffer = StringIO()
        traceback.print_exc(file=string_buffer)
        with open(output_file, 'w') as f:
            f.write(f'Error: {str(e)}')
            f.write(string_buffer.getvalue())
    finally:
        cursor.close()


def create_multivariate_stats(table, col_pairs):
    cursor = conn.cursor()
    for i, (col1, col2) in enumerate(col_pairs):
        cursor.execute(f'CREATE STATISTICS stats{i} ON {col1}, {col2} FROM {table};')
    cursor.execute(f'ANALYZE {table};')
    cursor.close()
    conn.commit()


def drop_multivariate_stats(table, col_pairs):
    cursor = conn.cursor()
    for i, _ in enumerate(col_pairs):
        cursor.execute(f'DROP STATISTICS stats{i};')
    cursor.close()
    conn.commit()


def explain(table: str, query_file: str, split: int):
    print(f'Running EXPLAIN on table {table}, query {query_file}')
    with open(os.path.join(QUERY_DIR, query_file)) as f:
        query = f.read()
    query = query.replace('`', '"')
    query = 'EXPLAIN (FORMAT TEXT) ' + query
    os.makedirs(os.path.join(RESULTS_DIR, table, str(split)), exist_ok=True)
    output_file = os.path.join(RESULTS_DIR, table, str(split), query_file)

    cursor = conn.cursor()
    try:
        cursor.execute(f"SET statement_timeout = {TIMEOUT_MS};")
        cursor.execute(query)
        conn.commit()
        results = cursor.fetchall()
        with open(output_file, 'w') as f:
            for row in results:
                f.write(f"{row[0]}\n")
    except psycopg2.extensions.QueryCanceledError:
        conn.rollback()
        print(f'Query {query_file} timed out after {TIMEOUT_MS} ms')
        with open(output_file, 'w') as f:
            f.write(f'Query {query_file} timed out after {TIMEOUT_MS} ms\n')
    except Exception as e:
        conn.rollback()
        print(f'Query {query_file} fails')
        string_buffer = StringIO()
        traceback.print_exc(file=string_buffer)
        with open(output_file, 'w') as f:
            f.write(f'Error: {str(e)}')
            f.write(string_buffer.getvalue())
    finally:
        cursor.close()


if __name__ == '__main__':
    for table in TARGET_TABLES:
        # for query_file in TABLE_TO_QUERY[table]:
        #     explain_analyze(table, query_file)

        col_pairs = list(filter(lambda x: x[0].startswith(PREFIX[table]), KL_LIST))
        col_pairs = [(x[0], x[1]) for x in col_pairs]
        n_pairs = len(col_pairs)
        # create statistics
        for i in range(N_SPLIT):
            split_pairs = col_pairs[i * (n_pairs // N_SPLIT):(i + 1) * (n_pairs // N_SPLIT)]
            create_multivariate_stats(table, split_pairs)
            for query_file in TABLE_TO_QUERY[table]:
                explain(table, query_file, i)
            drop_multivariate_stats(table, split_pairs)
