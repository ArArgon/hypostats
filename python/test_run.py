import sys

import duckdb
from datetime import datetime, timezone

sys.path.append('./python')

import psycopg2
from psycopg2 import sql
import pandas as pd
from test_correlation import *
from matplotlib import pyplot as plt


def get_column_metadata(conn, table, column, schema='public'):
    with conn.cursor() as cur:
        # 1) get data type
        cur.execute(
            sql.SQL("""
                SELECT data_type
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name   = %s
                  AND column_name  = %s
            """),
            [schema, table, column]
        )
        d = cur.fetchone()
        if not d:
            raise ValueError(f"Column {schema}.{table}.{column} not found")
        data_type = d[0]
        # 2) check for UNIQUE or PRIMARY KEY constraint
        cur.execute(
            sql.SQL("""
                SELECT EXISTS (
                  SELECT 1
                  FROM information_schema.table_constraints tc
                  JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                   AND tc.table_schema   = kcu.table_schema
                   AND tc.table_name     = kcu.table_name
                  WHERE tc.table_schema = %s
                    AND tc.table_name   = %s
                    AND kcu.column_name = %s
                    AND tc.constraint_type IN ('UNIQUE','PRIMARY KEY')
                )
            """),
            [schema, table, column]
        )
        is_unique = cur.fetchone()[0]
    return data_type, is_unique


def get_column_type(conn, table, column):
    info = conn.execute(f"PRAGMA table_info('{table}')").fetchall()
    # info rows: (column_index, name, type, nullable, default, ...)

    for idx, name, dtype, nullable, default, *_ in info:
        if name == column:
            return dtype
    raise RuntimeError(f'table {table} column {column} not found')


def construct_histograms(samples: pd.DataFrame, conn, table, max_bins=100):
    histograms = []
    for column in samples.columns:
        data_type = get_column_type(conn, table, column)
        if data_type == 'INTEGER':
            samples[column] = samples[column].fillna(-(1 << 64))
            samples[column] = samples[column].astype('int64')
        elif data_type == 'VARCHAR':
            samples[column] = samples[column].fillna('')
            samples[column] = samples[column].astype(str)
        elif data_type == 'DATE':
            samples[column] = samples[column].fillna('1970-01-01')
            samples[column] = samples[column].apply(lambda x: pd.to_datetime(x).timestamp()).astype('float64')
        elif data_type.startswith('DECIMAL'):
            samples[column] = samples[column].fillna(-float('inf'))
            samples[column] = samples[column].astype('float64')
        else:
            raise RuntimeError('unknown column data type')
        data = samples[column].tolist()
        data.sort()
        if len(data) <= max_bins:
            histograms.append(data)
        else:
            histograms.append(data[::len(data) // max_bins])
    return histograms


def compute_correlation(samples: pd.DataFrame, histograms):
    columns = list(samples.columns)
    kl_div = {}
    p_chi_square = {}
    p_spearman = {}
    p_pearson = {}
    for i in range(len(columns)):
        for j in range(i + 1, len(columns)):
            kl = test_kl_divergence(samples[columns[i]], histograms[i], samples[columns[j]], histograms[j])
            kl_div[(columns[i], columns[j])] = kl
            p_chi_square[(columns[i], columns[j])] = test_chi_square(samples[columns[i]], histograms[i], samples[columns[j]], histograms[j])
            p_spearman[(columns[i], columns[j])] = test_spearman(samples[columns[i]], histograms[i], samples[columns[j]], histograms[j])
            p_pearson[(columns[i], columns[j])] = test_pearson(samples[columns[i]], histograms[i], samples[columns[j]], histograms[j])
    return kl_div, p_chi_square, p_spearman, p_pearson


if __name__ == '__main__':
    # tables = ['cast_info', 'char_name', 'movie_info', 'name', 'person_info', 'title']
    tables = ['call_center', 'catalog_page', 'catalog_returns', 'catalog_sales', 'customer', 'customer_address', 'customer_demographics', 'date_dim', 'household_demographics', 'income_band', 'inventory', 'item', 'promotion', 'reason', 'ship_mode', 'store', 'store_returns', 'store_sales', 'time_dim', 'warehouse', 'web_page', 'web_returns', 'web_sales', 'web_site']

    kl_div = {}
    p_chi_square = {}
    p_spearman = {}
    p_pearson = {}

    conn = duckdb.connect('/Users/yifanlin/Desktop/MSIN/15799-query/tpc-ds/tpc-ds.db')
    for table in tables:
        print(f'processing table {table}')
        path = f'/Users/yifanlin/Desktop/MSIN/15799-query/tpc-ds/samples/{table}_sample.csv'
        samples = pd.read_csv(path)
        if len(samples) < 30000:
            continue
        histograms = construct_histograms(samples, conn, table)
        table_kl_div, table_p_chi_square, table_p_spearman, table_p_pearson = compute_correlation(samples, histograms)
        kl_div.update(table_kl_div)
        p_chi_square.update(table_p_chi_square)
        p_spearman.update(table_p_spearman)
        p_pearson.update(table_p_pearson)

    keys = list(kl_div.keys())
    kl_div_vals = [kl_div[key] for key in keys]
    p_chi_square_vals = [p_chi_square[key] for key in keys]
    p_spearman_vals = [p_spearman[key] for key in keys]
    p_pearson_vals = [p_pearson[key] for key in keys]
    plt.scatter(kl_div_vals, p_chi_square_vals, s=2, label='chi_square')
    plt.scatter(kl_div_vals, p_spearman_vals, s=2, label='spearman')
    plt.scatter(kl_div_vals, p_pearson_vals, s=2, label='pearson')
    plt.xlabel('KL Divergence')
    plt.ylabel('P-Values')
    plt.legend()
    plt.show()
    print('exit')