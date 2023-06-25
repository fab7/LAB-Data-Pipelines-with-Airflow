import pendulum
import logging

from airflow.decorators import dag, task

@dag(
    schedule_interval='@hourly',
    start_date=pendulum.now()
)
def task_dependencies():

    #===========================================
    #== TASKS DEFINITION SECTION
    #===========================================
    @task()
    def hello_world():
        logging.info("Hello World -- I am L2_E3 - ")

    @task()
    def addition(first, second):
        logging.info(f"{first} + {second} = {first+second}")
        return first+second

    @task()
    def subtraction(first, second):
        logging.info(f"{first -second} = {first-second}")
        return first-second

    @task()
    def division(first, second):
        logging.info(f"{first} / {second} = {int(first/second)}")   
        return int(first/second)     

    #===========================================
    #== TASKS INVOCATION SECTION #1
    #===========================================
    
    # Call the hello world task function
    #  Note: 'hello' represents a discrete invocation of 'hello_world()'
    hello = hello_world()

    # Call the addition function with some constants (numbers)
    #  Note: 'two_plus_two' represents the invocation of 'addition()' with 2 and 2
    two_plus_two = addition(2, 2)

    # Call the subtraction function with some constants (numbers)
    #  Note: 'two_from_six' represents the invocation of 'subtraction()' with 6 and 2
    two_from_six = subtraction(6, 2)

    # Call the division function with some constants (numbers)
    #  Note: 'eight_divided_by_two' represents the invocation of 'division()' with 8 and 2
    eight_divided_by_two = division(8, 2)

    #========================================================
    #== DEPENDENCY GRAPH #1
    #========================================================
    #
    #                    ->  addition_task
    #                   /                 \
    #   hello_world_task                   -> division_task
    #                   \                 /
    #                    ->subtraction_task
    #
    #========================================================
    hello >> two_plus_two
    hello >> two_from_six

    two_plus_two >> eight_divided_by_two
    two_from_six >> eight_divided_by_two
    
    #===========================================
    #== TASKS INVOCATION SECTION #2
    #===========================================
    
    # Call the addition function with some constants (numbers)
    # and assign the result of the subtraction function to a variable
    sum = addition(5,5)

    # Call the subtraction function with some constants (numbers)
    # and assign the result of the subtraction function to a variable
    difference = subtraction(6, 4)

    # Call the division function and pass the result of the addition function, 
    # and the subtraction function to the division function
    sum_divided_by_difference = division(sum, difference)

    #========================================================
    #== DEPENDENCY GRAPH #2
    #========================================================
    #
    #     ->   addition_task
    #                       \
    #                        -> division_task
    #                       /
    #     ->subtraction_task
    #
    #========================================================
    sum        >> sum_divided_by_difference
    difference >> sum_divided_by_difference


task_dependencies_dag=task_dependencies()
