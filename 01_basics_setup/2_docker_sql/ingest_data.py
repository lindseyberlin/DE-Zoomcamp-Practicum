#!/usr/bin/env python
# coding: utf-8

import argparse
import pandas as pd
import os
from time import time
from sqlalchemy import create_engine

def main(params):
    
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db_name = params.db_name
    table_name = params.table_name
    csv_url = params.csv_url

    csv_name = 'output.csv'

    os.system(f"wget {csv_url} -O {csv_name}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db_name}")
    engine.connect()


    # reading in the data in 100K row chunks
    df_intake = pd.read_csv(
        csv_name, 
        iterator=True, 
        chunksize=100000, 
        compression='gzip')

    df_chunk = next(df_intake)

    for col in ["tpep_pickup_datetime", "tpep_dropoff_datetime"]:
        df_chunk[col] = pd.to_datetime(df_chunk[col])

    df_chunk.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")

    df_chunk.to_sql(name=table_name, con=engine, if_exists="append")

    # not the cleanest code, but what was provided from course
    # loops through iterator chunks to insert data into database

    n = 1
    while True:
        n += 1
        t_start = time()
        df_chunk = next(df_intake)
        
        for col in ["tpep_pickup_datetime", "tpep_dropoff_datetime"]:
            df_chunk[col] = pd.to_datetime(df_chunk[col])

        df_chunk.to_sql(name=table_name, con=engine, if_exists="append")

        t_end = time()
        print(f"Completed chunk {n} in {t_end-t_start} seconds")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Ingest CSV data to Postgres (messily)')

    # arguments needed:
    # user, password, host, port, database name, URL of csv
    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db_name', help='database name for postgres')
    parser.add_argument('--table_name', help='database name where we will write results to')
    parser.add_argument('--csv_url', help='url of the csv file')

    args = parser.parse_args()

    main(args)
