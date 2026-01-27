import pandas as pd
from sqlalchemy import create_engine

dates = ["lpep_pickup_datetime", "lpep_dropoff_datetime"]

def run():
    df = pd.read_parquet(
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet",
        engine="pyarrow"
    )

    for date in dates:
        df[date] = pd.to_datetime(df[date])

    engine = create_engine("postgresql://root:root@localhost:5432/ny_taxi")

    df.head(0).to_sql("green_trips", engine, if_exists="replace", index=False)
    df.to_sql("green_trips", engine, if_exists="append", index=False, chunksize=100_000)

    zones = pd.read_csv(
        "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
    )
    zones.head(0).to_sql("zones", engine, if_exists="replace", index=False)
    zones.to_sql("zones", engine, if_exists="append", index=False)

if __name__ == "__main__":
    run()
