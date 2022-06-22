from dagster import repository

from etl.jobs.say_hello import say_hello_job
from etl.jobs.etl_jobs import job_main
from etl.schedules.my_hourly_schedule import my_hourly_schedule
from etl.schedules.etl_schedules import job_main_extract
from etl.sensors.my_sensor import my_sensor


@repository
def etl():
    """
    The repository definition for this etl Dagster repository.

    For hints on building your Dagster repository, see our documentation overview on Repositories:
    https://docs.dagster.io/overview/repositories-workspaces/repositories
    """
    jobs = [say_hello_job, job_main_extract]
    schedules = [my_hourly_schedule, job_main]
    sensors = [my_sensor]

    return jobs + schedules + sensors
