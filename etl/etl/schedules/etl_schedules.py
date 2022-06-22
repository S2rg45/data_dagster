from dagster import schedule

from etl.jobs.etl_jobs import job_main


@schedule(cron_schedule="* 20 * * *", job=job_main, execution_timezone="America/Bogota")
def job_main_extract():
  run_config = {}
  return run_config

