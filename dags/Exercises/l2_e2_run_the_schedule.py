import pendulum
import logging

from airflow.decorators import dag, task

#
# A DAG with daily `schedule_interval` argument
#
@dag(
    start_date=pendulum.now(),
    schedule_interval='@daily'
)
def greet_flow():
    
    @task
    def hello_world():
        logging.info("Hello World -- I am L2_E2 -- I run on a daily schedule!")

    # hello_world represents the invocation of the only task in this DAG
    # it will run by itself, without any sequence before or after another task
    hello_world_task=hello_world()

greet_flow_dag=greet_flow()
