import logging
import pendulum

#-----------------------------------------------------------------------------
# ABOUT PENDULUM
#-----------------------------------------------------------------------------
# The Pendulum library is a Python library that simplifies working with dates,
#  times, and timezones in Python. It provides a more intuitive and easier-to-
#  use API for handling dates, times, and timezones compared to the built-in
#  datetime module.
# Pendulum offers features such as:
#  - Easy creation and manipulation of dates and times: Pendulum allows you to
#    create date and time objects using various methods, such as parsing
#    strings, specifying individual components, or using relative expressions
#    like "next Monday".
#  - Timezone support: Pendulum handles timezones seamlessly, allowing you to
#    easily convert between different timezones and perform timezone-aware
#    calculations.
#  - Fluent interface: Pendulum provides a fluent interface that allows you to
#    chain methods together, making it easy to perform complex operations on
#    dates and times.
#  - Human-readable output: Pendulum provides methods to format dates and
#    times in a human-readable way, such as "2 days ago" or "in 1 hour".
#-----------------------------------------------------------------------------

from airflow.decorators import dag, task

#-----------------------------------------------------------------------------
# ABOUT @DAG
#-----------------------------------------------------------------------------
# The @dag decorator is used in Apache Airflow to define a Directed Acyclic
#  Graph (DAG). A DAG is a collection of tasks that are organized in a 
#  specific order and have dependencies on each other. The @dag decorator is
#  used to define the structure and properties of the DAG.
# In Airflow, a DAG is represented as a Python script, and the @dag decorator
#  is used to define a function as a DAG. The decorator takes in various
#  arguments to configure the DAG, such as the DAG ID, the start date, the
#  schedule interval, and more.
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
# ABOUT @TASK
#-----------------------------------------------------------------------------
# The @task decorator is used in Apache Airflow to define a task within a DAG
#  (Directed Acyclic Graph). A task represents a unit of work that needs to be
#  executed as part of a data pipeline. The @task decorator is used to define
#  a function as a task and specify its properties.
# The @task decorator can also take additional arguments to configure the task,
#  such as the task ID, retries, retry delay, and more.
#-----------------------------------------------------------------------------



# The @dag decorates the 'greet_task' to denote it's the main function
@dag(
    start_date=pendulum.now()
)
def greet_flow_dag():
  
    # The @task decorates the re-usable 'hello_world_task' 
    # It can be called as often as needed in the DAG
    @task
    def hello_world_task():
        logging.info("Hello World -- I am L2_E1 -- This is my very first program in Airflow!")

    # 'hello_world' represents a discrete invocation of the 'hello_world_task'
    hello_world=hello_world_task()

# The 'greet_dag' represents the invocation of the 'greet_flow_dag'
greet_dag=greet_flow_dag()
