from dagster import  job
from etl.ops.etl import get_data, load_data

@job
def job_main():
    df, name_table = get_data()
    load_data(df, name_table)
  