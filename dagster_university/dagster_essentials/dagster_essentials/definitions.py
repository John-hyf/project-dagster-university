import dagster as dg

from dagster_essentials.assets import metrics, trips, requests
from dagster_essentials.resources import duckdb_resource
from dagster_essentials.jobs import daily_update_trip_job, weekly_trip_update_job, adhoc_request_job
from dagster_essentials.schedules import daily_trip_update_schedule, weekly_trip_update_schedule
from  dagster_essentials.sensors import adhoc_request_sensor


trip_assets = dg.load_assets_from_modules([trips])
metric_assets = dg.load_assets_from_modules([metrics])
request_assets = dg.load_assets_from_modules([requests])

all_jobs = [daily_update_trip_job, weekly_trip_update_job, adhoc_request_job]
all_schedules = [daily_trip_update_schedule, weekly_trip_update_schedule]
all_sensors = [adhoc_request_sensor]

defs = dg.Definitions(
    assets=[*trip_assets, *metric_assets, *request_assets],
    resources={'duckdb_res': duckdb_resource},
    jobs=all_jobs,
    sensors=all_sensors,
    schedules=all_schedules
)
