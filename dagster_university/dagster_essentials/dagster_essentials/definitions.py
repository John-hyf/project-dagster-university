import dagster as dg

from dagster_essentials.assets import metrics, trips
from dagster_essentials.resources import duckdb_resource
from dagster_essentials.jobs import daily_update_trip_job, weekly_trip_update_job
from dagster_essentials.schedules import daily_trip_update_schedule, weekly_trip_update_schedule


trip_assets = dg.load_assets_from_modules([trips])
metric_assets = dg.load_assets_from_modules([metrics])

trip_jobs = [daily_update_trip_job, weekly_trip_update_job]
trip_schedules = [daily_trip_update_schedule, weekly_trip_update_schedule]

defs = dg.Definitions(
    assets=[*trip_assets, *metric_assets],
    resources={'duckdb_res': duckdb_resource},
    jobs=trip_jobs,
    schedules=trip_schedules
)
