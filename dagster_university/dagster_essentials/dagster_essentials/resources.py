import dagster as dg
from dagster_duckdb import DuckDBResource


duckdb_resource = DuckDBResource(database=dg.EnvVar("DUCKDB_DATABASE"))
