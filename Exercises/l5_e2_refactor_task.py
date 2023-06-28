# INSTTRUCTION
#  In this exercise, we’ll refactor a DAG with a single overloaded task into a
#  DAG with several tasks with well-defined boundaries
#   1 - Read through the DAG and identify points in the DAG that could be split
#       apart
#   2 - Split the DAG into multiple tasks
#   3 - Add necessary dependency flows for the new tasks
#   4 - Run the DAG

import pendulum
import logging

from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators.postgres_operator import PostgresOperator


@dag (
    start_date=pendulum.now()
)
def demonstrating_refactoring():

    #
    # BEFORE refactoring this function is a long list of sub-tasks.
    #
    @task()
    def long_version_of_load_and_analyze(*args, **kwargs):
        redshift_hook = PostgresHook("redshift")

        # Find all trips where the rider was under 18
        redshift_hook.run(
            """
            BEGIN;
            DROP TABLE IF EXISTS younger_riders;
            CREATE TABLE younger_riders AS (
                SELECT * FROM trips WHERE birthyear > 2000
            );
            COMMIT;
            """)
        records = redshift_hook.get_records(
            """
            SELECT birthyear FROM younger_riders ORDER BY birthyear DESC LIMIT 1
            """)
        if len(records) > 0 and len(records[0]) > 0:
            logging.info(f"Youngest rider was born in {records[0][0]}")

        # Find out how often each bike is ridden
        redshift_hook.run(
            """
            BEGIN;
            DROP TABLE IF EXISTS lifetime_rides;
            CREATE TABLE lifetime_rides AS (
                SELECT bikeid, COUNT(bikeid)
                FROM trips
                GROUP BY bikeid
            );
            COMMIT;
            """)

        # Count the number of stations by city
        redshift_hook.run(
            """
            BEGIN;
            DROP TABLE IF EXISTS city_station_counts;
            CREATE TABLE city_station_counts AS(
                SELECT city, COUNT(city)
                FROM stations
                GROUP BY city
            );
            COMMIT;
            """)

    #
    # AFTER refactoring this function into an appropriate set of 4 tasks.
    #  1- find_riders_under_18
    #  2- how_often_bikes_ridden
    #  3- create_station_count
    #
    @task()
    def find_riders_under_18(*args, **kwargs):
        """
        Find all riders under 18.
        """
        redshift_hook = PostgresHook("redshift")       
        redshift_hook.run(
            """
            BEGIN;
            DROP TABLE IF EXISTS younger_riders;
            CREATE TABLE younger_riders AS (
                SELECT * FROM trips WHERE birthyear > 2000
            );
            COMMIT;
            """)
        records = redshift_hook.get_records(
            """
            SELECT birthyear FROM younger_riders ORDER BY birthyear DESC LIMIT 1
            """
            )
        if len(records) > 0 and len(records[0]) > 0:
            logging.info(f"Youngest rider was born in {records[0][0]}")

    @task()
    def how_often_bikes_ridden(*args, **kwargs):
        """
        Find out how often each bike is ridden.
        """
        redshift_hook = PostgresHook("redshift")    
        redshift_hook.run(
            """
            BEGIN;
            DROP TABLE IF EXISTS lifetime_rides;
            CREATE TABLE lifetime_rides AS (
                SELECT bikeid, COUNT(bikeid)
                FROM trips
                GROUP BY bikeid
            );
            COMMIT;
            """
            )

    @task()
    def create_station_count(*args, **kwargs):
        """
        Count the number of stations by city.
        """        
        redshift_hook = PostgresHook("redshift")       
        redshift_hook.run(
            """
            BEGIN;
            DROP TABLE IF EXISTS city_station_counts;
            CREATE TABLE city_station_counts AS(
                SELECT city, COUNT(city)
                FROM stations
                GROUP BY city
            );
            COMMIT;
            """)

    @task()
    def log_oldest():
        """
        Log the oldest rider.
        """
        redshift_hook = PostgresHook("redshift")
        records = redshift_hook.get_records(
            """
            SELECT birthyear FROM older_riders ORDER BY birthyear ASC LIMIT 1
            """)
        if len(records) > 0 and len(records[0]) > 0:
            logging.info(f"Oldest rider was born in {records[0][0]}")
 
    #===========================================
    #== TASKS INVOCATION SECTION
    #===========================================
    # OBSOLETE load_and_analyze = long_version_of_load_and_analyze()
    task_find_riders_under_18   = find_riders_under_18()
    task_how_often_bikes_ridden = how_often_bikes_ridden()
    task_create_station_count   = create_station_count()
    task_log_oldest             = log_oldest()

    task_drop_oldest            = PostgresOperator(
        task_id="drop_oldest",
        postgres_conn_id="redshift",
        sql="""
            DROP TABLE IF EXISTS older_riders;
            """
        )
        
    #task_create_oldest          = PostgresOperator(
    #    task_id="create_oldest",
    #    sql="""
    #        BEGIN;
    #        DROP TABLE IF EXISTS older_riders;
    #        CREATE TABLE older_riders AS (
    #            SELECT * FROM trips WHERE birthyear > 0 AND birthyear <= 1945
    #        );
    #        COMMIT;
    #        """,
    #    postgres_conn_id="redshift")

    task_create_oldest          = PostgresOperator(
        task_id="create_oldest",
        postgres_conn_id="redshift",
        sql="""
            CREATE TABLE older_riders AS (
                SELECT * FROM trips WHERE birthyear > 0 AND birthyear <= 1945);
            """
        )

    #========================================================
    #== DEPENDENCY GRAPH
    #========================================================
    #OBSOLETE load_and_analyze   >> create_oldest_task

    task_find_riders_under_18   >> task_drop_oldest
    task_how_often_bikes_ridden >> task_drop_oldest
    task_create_station_count   >> task_drop_oldest

    task_drop_oldest            >> task_create_oldest
    task_create_oldest          >> task_log_oldest

demonstrating_refactoring_dag = demonstrating_refactoring()
