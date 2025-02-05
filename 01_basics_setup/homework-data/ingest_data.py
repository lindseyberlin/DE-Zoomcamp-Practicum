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
    taxi_table_name = params.taxi_table_name
    zone_table_name = params.zone_table_name
    green_taxi_url = params.green_taxi_url
    zone_url = params.zone_url


    green_csv_name = 'green_output.csv'
    zone_csv_name = 'zone_output.csv'

    os.system(f"wget {green_taxi_url} -O {green_csv_name}")
    os.system(f"wget {zone_url} -O {zone_csv_name}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db_name}")
    engine.connect()

    t_start = time()

    # first handling the zone data
    df_zone = pd.read_csv(zone_csv_name)

    df_zone.to_sql(name=zone_table_name, con=engine, if_exists="replace")

    t_end = time()

    print(f"Completed zone table in {t_end-t_start} seconds")
    print("Now reading in green taxi data")

    # now reading in the green taxi trip data in 100K row chunks
    df_intake = pd.read_csv(
        green_csv_name, 
        iterator=True, 
        chunksize=100000, 
        compression='gzip')

    df_chunk = next(df_intake)

    datetime_cols = [c for c in df_chunk.columns if "datetime" in c]

    for col in datetime_cols:
        df_chunk[col] = pd.to_datetime(df_chunk[col])

    df_chunk.head(n=0).to_sql(name=taxi_table_name, con=engine, if_exists="replace")

    df_chunk.to_sql(name=taxi_table_name, con=engine, if_exists="append")

    # not the cleanest code, but what was provided from course
    # loops through iterator chunks to insert data into database

    n = 1
    while True:
        n += 1
        t_start = time()
        df_chunk = next(df_intake)
        
        for col in datetime_cols:
            df_chunk[col] = pd.to_datetime(df_chunk[col])

        df_chunk.to_sql(name=taxi_table_name, con=engine, if_exists="append")

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
    parser.add_argument('--taxi_table_name', help='database name for taxi results')
    parser.add_argument('--zone_table_name', help='database name for zone results')
    parser.add_argument('--green_taxi_url', help='url of the green taxi csv.gz file')
    parser.add_argument('--zone_url', help='url of the zones csv file')

    args = parser.parse_args()

    main(args)
