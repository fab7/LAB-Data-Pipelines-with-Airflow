# INSTRUCTIONS
# 1 - Revisit our bikeshare traffic 
# 2 - Update our DAG with
#   a - @monthly schedule_interval
#   b - max_active_runs of 1
#   c - start_date of 2018/01/01
#   d - end_date of 2018/02/01
#  Use Airflow’s backfill capabilities to analyze our trip data on a monthly basis over 2 historical runs

# Remember to run "/opt/airflow/start.sh" command to start the web server. 
#  Once the Airflow web server is ready,  open the Airflow UI using the "Access Airflow" button. 
#  Turn your DAG “On”, and then Run your DAG.

import pendulum

from airflow.decorators import dag, task
from airflow.secrets.metastore import MetastoreBackend
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from udacity.common import sql_statements

@dag(
    # Set the start date
    start_date=pendulum.datetime(2018, 1, 1, 0, 0, 0, 0),
    # Set the end date to February first
    end_date=pendulum.datetime(2018, 2, 1, 0, 0, 0, 0),
    # Set the schedule to be monthly
    schedule_interval='@monthly',
    # Set the number of max active runs to 1
    # Note:  The 'max_active_runs' specifies the maximum number of DAG runs that
    #  can be in the "running" state simultaneously. By default, 'max_active_runs'
    #  is set to 16, which means that Airflow will allow up to 16 concurrent runs
    #  of the same DAG. If the number of active runs exceeds this limit, any 
    #  additional runs will be queued and will start running only when one of the
    #  active runs completes.
    #  The 'max_active_runs' parameter is useful when you want to limit the 
    #  resources consumed by a DAG or when you want to prevent too many concurrent
    #  runs from overwhelming your system. It helps in managing the execution of
    #  DAGs and ensures that the system doesn't get overloaded with too many
    #  running tasks.
    #  It's important to note that the max_active_runs parameter is set at the
    #  DAG level, so it applies to all tasks within the DAG. If you want to set
    #  different limits for different tasks within the same DAG, you can use
    #  task-level parameters like task_concurrency.
    max_active_runs=1
)
def schedule_backfills():

    @task()
    def load_trip_data_to_redshift(*args, **kwargs):
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection("aws_credentials")

        redshift_hook = PostgresHook("redshift")
        sql_stmt = sql_statements.COPY_ALL_TRIPS_SQL.format(
            aws_connection.login,
            aws_connection.password,
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

    create_trips_table = PostgresOperator(
        task_id="create_trips_table",
        postgres_conn_id="redshift",
        sql=sql_statements.CREATE_TRIPS_TABLE_SQL
    )

    create_stations_table = PostgresOperator(
        task_id="create_stations_table",
        postgres_conn_id="redshift",
        sql=sql_statements.CREATE_STATIONS_TABLE_SQL,
    )    

    load_trips_task = load_trip_data_to_redshift()
    load_stations_task = load_station_data_to_redshift()


    create_trips_table >> load_trips_task
    create_stations_table >> load_stations_task

schedule_backfills_dag = schedule_backfills()
