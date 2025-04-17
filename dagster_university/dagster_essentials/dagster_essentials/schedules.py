import dagster as dg
from dagster_essentials.jobs import daily_update_trip_job, weekly_trip_update_job


daily_trip_update_schedule = dg.ScheduleDefinition(
    job=daily_update_trip_job,
    cron_schedule="20 16 * * *"
)


weekly_trip_update_schedule = dg.ScheduleDefinition(
    job=weekly_trip_update_job,
    cron_schedule="0 0 * * 1"
)
