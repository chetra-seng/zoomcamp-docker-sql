#!/usr/bin/env python
# coding: utf-8


import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

year = "2021"
month = "01"
url = f"{prefix}/yellow/yellow_tripdata_{year}-{month}.csv.gz"

db_user = "root"
db_password = "root"
db_host = "localhost"
db_port = 5432
db_name = "ny_taxi"

target_table = "yellow_taxi_data"


def run():
    dtype = {
        "VendorID": "Int64",
        "passenger_count": "Int64",
        "trip_distance": "float64",
        "RatecodeID": "Int64",
        "store_and_fwd_flag": "string",
        "PULocationID": "Int64",
        "DOLocationID": "Int64",
        "payment_type": "Int64",
        "fare_amount": "float64",
        "extra": "float64",
        "mta_tax": "float64",
        "tip_amount": "float64",
        "tolls_amount": "float64",
        "improvement_surcharge": "float64",
        "total_amount": "float64",
        "congestion_surcharge": "float64",
    }

    parse_dates = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

    engine = create_engine(
        f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )

    df_iter = pd.read_csv(
        url,
        dtype=dtype,  # type: ignore
        parse_dates=parse_dates,
        iterator=True,
        chunksize=100000,
    )

    first = True
    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(n=0).to_sql(
                name=target_table, con=engine, if_exists="replace"
            )
            first = False

        df_chunk.to_sql(name=target_table, con=engine, if_exists="append")


if __name__ == "__main__":
    run()
