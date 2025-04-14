import requests
# import os
# import duckdb
# from dagster._utils.backoff import backoff

import dagster as dg
from dagster_essentials.assets import constants
from dagster_essentials.resources import DuckDBResource


@dg.asset(
    group_name="ods_fs",
    owners=['John.HYF@gamil.com'],
    kinds={'fs', 'parquet'}
)
def taxi_trips_file() -> None:
    """
    The raw parquet file for the taxi trips dataset.
    Sourced from the NYC Open Data portal.
    """
    month_to_fetch = '2023-03'
    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )
    with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), 'wb') as of:
        of.write(raw_trips.content)



@dg.asset(
    group_name="ods_fs",
    owners=['John.HYF@gamil.com'],
    kinds={'fs', 'csv'}
)
def taxi_zones_file() -> None:
    """
    The raw csv file for the taxi zones dataset.
    Sourced from the NYC Open Data portal.
    """
    raw_zones = requests.get(
        "https://community-engineering-artifacts.s3.us-west-2.amazonaws.com/dagster-university/data/taxi_zones.csv"
    )
    with open(constants.TAXI_ZONES_FILE_PATH, 'wb') as of:
        of.write(raw_zones.content)


@dg.asset(
    group_name="ods_duckdb",
    owners=['John.HYF@gamil.com'],
    deps=['taxi_trips_file'],
    kinds={'duckdb'}
)
def taxi_trips(duckdb_res: DuckDBResource) -> None:
    """
      The raw taxi trips dataset, loaded into a DuckDB database
    """
    query = """
        create or replace table trips as (
          select
            VendorID as vendor_id,
            PULocationID as pickup_zone_id,
            DOLocationID as dropoff_zone_id,
            RatecodeID as rate_code_id,
            payment_type as payment_type,
            tpep_dropoff_datetime as dropoff_datetime,
            tpep_pickup_datetime as pickup_datetime,
            trip_distance as trip_distance,
            passenger_count as passenger_count,
            total_amount as total_amount
          from 'data/raw/taxi_trips_2023-03.parquet'
        );
    """
    # version 1 with raw connector
    # conn = backoff(
    #     fn=duckdb.connect,
    #     retry_on=(RuntimeError, duckdb.IOException),
    #     kwargs={
    #         "database": os.getenv("DUCKDB_DATABASE"),
    #     },
    #     max_retries=3,
    # )
    # conn.execute(query)

    # version 2 with DBResource
    with duckdb_res.get_connection() as conn:
        conn.execute(query)


@dg.asset(
    group_name="ods_duckdb",
    owners=['John.HYF@gamil.com'],
    deps=['taxi_zones_file'],
    kinds={'duckdb'}
)
def taxi_zones(duckdb_res: DuckDBResource) -> None:
    """
      The raw taxi zones dataset, loaded into a DuckDB database
    """
    query = f"""
        create or replace table zones as (
            select
                LocationID as zone_id,
                zone,
                borough,
                the_geom as geometry
            from '{constants.TAXI_ZONES_FILE_PATH}'
        );
    """
    # # TODO: has concurrency problem
    # conn = backoff(
    #     fn=duckdb.connect,
    #     retry_on=(RuntimeError, duckdb.IOException),
    #     kwargs={
    #         "database": os.getenv("DUCKDB_DATABASE"),
    #     },
    #     max_retries=3,
    # )
    # conn.execute(query)

    with duckdb_res.get_connection() as conn:
        conn.execute(query)
