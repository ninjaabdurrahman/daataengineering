#!/usr/bin/env python
# coding: utf-8
import argparse
import os
from time import perf_counter

import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.host
    db_name = params.db_name
    table_name = params.table_name
    url = params.url

    # download csv

    file_name = url.split("/")[-1]

    # os.system(f"wget {url} -O {csv_name}")
    os.system(f"wget -q {url} && gzip -d {file_name}")  # && rm {file_name})

    file_name = file_name.split(".")[0] + ".csv"

    ## rename csv
    os.rename(file_name, "output.csv")

    csv_name = "output.csv"

    engine = create_engine(
        f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
    )

    _iter = pd.read_csv(
        csv_name,
        iterator=True,
        chunksize=100000,
        error_bad_lines="corece",
        low_memory=False,
    )

    for chunk in _iter:
        t_start = perf_counter()

        chunk.tpep_dropoff_datetime = pd.to_datetime(
            chunk.tpep_dropoff_datetime
        )
        chunk.tpep_pickup_datetime = pd.to_datetime(chunk.tpep_pickup_datetime)

        chunk.to_sql(name=table_name, con=engine, if_exists="append")

        t_end = perf_counter()
        print(f"Inserted another chunk, took {t_end - t_start} seconds")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ingest taxi data csv into postgres"
    )
    parser.add_argument("--user", help="user name for postgres")
    parser.add_argument("--password", help="password for postgres")
    parser.add_argument("--host", help="host for postgres")
    parser.add_argument("--port", help="port for postgres", default=5432)
    parser.add_argument("--db_name", help="database name")
    parser.add_argument(
        "--table_name", help="table name where the results will be ingested"
    )
    parser.add_argument("--url", help="url for the data")
    args = parser.parse_args()
    main(args)
