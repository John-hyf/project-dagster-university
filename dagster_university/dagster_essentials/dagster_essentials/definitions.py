import dagster as dg

from dagster_essentials.assets import metrics, trips
from dagster_essentials.resources import duckdb_resource


trip_assets = dg.load_assets_from_modules([trips])
metric_assets = dg.load_assets_from_modules([metrics])

defs = dg.Definitions(
    assets=[*trip_assets, *metric_assets],
    resources={'duckdb_res': duckdb_resource}
)
