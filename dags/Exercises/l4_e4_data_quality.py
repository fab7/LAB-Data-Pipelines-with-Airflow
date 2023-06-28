# INSTRUCTIONS
# 1 - Set an SLA on the max amount of time it should take to load trip data from s3 to redshift, say an hour
# 2 - Add data verification step after the load trip data from s3 to redshift
# 3 - Add data verification step after we load station data

import pendulum
import datetime
import logging

from airflow.decorators import dag,task
from airflow.secrets.metastore import MetastoreBackend
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from udacity.common import sql_statements

@dag(
    start_date=pendulum.now(),
    max_active_runs=1    
)
def data_quality():

# Add the task SLA as a parameter in the @task annotation
    @task()
    def load_trip_data_to_redshift(*args, **kwargs):
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection("aws_credentials")
        redshift_hook = PostgresHook("redshift")
        sql_stmt = sql_statements.COPY_ALL_TRIPS_SQL.format(
            aws_connection.login,
            aws_connection.password
        )
        redshift_hook.run(sql_stmt)

    @task()
    def load_station_data_to_redshift(*args, **kwargs):
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection("aws_credentials")
        redshift_hook = PostgresHook("redshift")
        sql_stmt = sql_statements.COPY_STATIONS_SQL.format(
            aws_connection.login,
            aws_connection.password,
        )
        redshift_hook.run(sql_stmt)

    @task()
    def check_greater_than_zero(*args, **kwargs):
        """
        Make sure that we are actually loading the data.
        """
        table = kwargs["params"]["table"]
        redshift_hook = PostgresHook("redshift")
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {table} returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {table} contained 0 rows")
        logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")


    create_trips_table = PostgresOperator(
        task_id="create_trips_table",
        postgres_conn_id="redshift",
        sql=sql_statements.CREATE_TRIPS_TABLE_SQL
    )

    load_trips_task = load_trip_data_to_redshift()


    check_trips_task = check_greater_than_zero(
        params={
            'table':'trips'
        }
    )

    create_stations_table = PostgresOperator(
        task_id="create_stations_table",
        postgres_conn_id="redshift",
        sql=sql_statements.CREATE_STATIONS_TABLE_SQL,
    )

    load_stations_task = load_station_data_to_redshift()

    check_stations_task = check_greater_than_zero(
        params={
            'table': 'stations',
        }
    )


    create_trips_table >> load_trips_task
    create_stations_table >> load_stations_task

#
# TODO: Set the task dependencies for the stations and trips check tasks
#

data_quality_dag = data_quality()

