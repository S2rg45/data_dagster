import psycopg2
import os
from dotenv import dotenv_values
from sqlalchemy import create_engine

def connection():
  config = dotenv_values(".env") 
  dictionary_config = dict(config)
  host = dictionary_config["DAGSTER_PG_HOST"]
  port = dictionary_config["DAGSTER_PG_PORT"]
  user = dictionary_config["DAGSTER_PG_USERNAME"]
  password = dictionary_config["DAGSTER_PG_PASSWORD"]
  db = dictionary_config["DAGSTER_PG_DB"]
  conn_string = 'postgresql://{}:{}@{}:{}/{}'.format(user, password,host,port, db)
  db = create_engine(conn_string)
  conn = db.connect()
  connection = psycopg2.connect(conn_string)
  return [connection, conn]


