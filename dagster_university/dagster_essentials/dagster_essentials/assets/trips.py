import requests
# import os
# import duckdb
# from dagster._utils.backoff import backoff

import dagster as dg
from dagster_essentials.assets import constants
from dagster_essentials.resources import DuckDBResource
from dagster_essentials.partitions import monthly_partition


@dg.asset(
    group_name="ods_fs",
    owners=['John.HYF@gamil.com'],
    kinds={'file', 'parquet'},
    partitions_def=monthly_partition
)
def taxi_trips_file(context: dg.AssetExecutionContext) -> None:
    """
    The raw parquet file for the taxi trips dataset.
    Sourced from the NYC Open Data portal.
    """
    context.log.debug(context)
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]
    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )
    with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), 'wb') as of:
        of.write(raw_trips.content)



@dg.asset(
    group_name="ods_fs",
    owners=['John.HYF@gamil.com'],
    kinds={'file', 'csv'}
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
    kinds={'duckdb'},
    partitions_def=monthly_partition
)
def taxi_trips(context: dg.AssetExecutionContext, duckdb_res: DuckDBResource) -> None:
    """
      The raw taxi trips dataset, loaded into a DuckDB database
    """
    # version 1
    # query = """
    #     create or replace table trips as (
    #       select
    #         VendorID as vendor_id,
    #         PULocationID as pickup_zone_id,
    #         DOLocationID as dropoff_zone_id,
    #         RatecodeID as rate_code_id,
    #         payment_type as payment_type,
    #         tpep_dropoff_datetime as dropoff_datetime,
    #         tpep_pickup_datetime as pickup_datetime,
    #         trip_distance as trip_distance,
    #         passenger_count as passenger_count,
    #         total_amount as total_amount
    #       from 'data/raw/taxi_trips_2023-03.parquet'
    #     );
    # """
    # version 2
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]

    query = f"""
        create table if not exists trips (
            vendor_id integer, pickup_zone_id integer, dropoff_zone_id integer,
            rate_code_id double, payment_type integer, dropoff_datetime timestamp,
            pickup_datetime timestamp, trip_distance double, passenger_count double,
            total_amount double, partition_date varchar
        );

        delete from trips where partition_date = '{month_to_fetch}';

        insert into trips
        select
            VendorID, PULocationID, DOLocationID, RatecodeID, payment_type, tpep_dropoff_datetime,
            tpep_pickup_datetime, trip_distance, passenger_count, total_amount, '{month_to_fetch}' as partition_date
        from '{constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)}';
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
