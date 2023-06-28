import pendulum
import logging

from airflow.decorators import dag, task
from airflow.secrets.metastore import MetastoreBackend

# NOTE: Because Redshift is very compatible with Postgres we can leverage the 
#   ProstgresHook as well as the PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator


# INSTRUCTIONS
#  - Use the PostgresHook to create a connection using the Redshift credentials from Airflow 
# - Use the PostgresOperator to create the Trips table
# - Use the PostgresOperator to run the LOCATION_TRAFFIC_SQL


from udacity.common import sql_statements

@dag(
    start_date=pendulum.now()
)
def load_data_to_redshift():

    #===========================================
    #== TASKS DEFINITION SECTION
    #===========================================
    @task
    def load_task():
        """
        Connect to the Airflow Metastore and retrive credentials (and/or other
         data needed to connect to outside systems).

        How: 
         - Use the 'MetastoreBackend' python class to connect to the Airflow 
         Metastore.
         - Create an 'aws_connection' object. It will contain the the Access 
         Key ID in 'aws_connection.login' and the Secret Access Key in 
         'aws_connection.password'.
        """
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection("aws_credentials")
        logging.info(vars(aws_connection))

        """
        Create a 'redshift_hook' variable that contains all the connection
         details for the Postgres database. 
        
        How:
         - Use the 'PostgresHook()' class. It retrieves the details 
         from the Postgres connection we created earlier in the Airflow UI.
        """
        redshift_hook = PostgresHook("redshift")
        
        """
        Execute the SQL statement 'COPY_ALL_TRIPS_SQL' (defined in udacity/common/sql_statements.py)
        on the Redshift cluster. This will load/copy the 'trips' from 's3://fab-se4s-bucket/...'
        into Redshift.
        """
        redshift_hook.run(sql_statements.COPY_ALL_TRIPS_SQL.format(
                            aws_connection.login, aws_connection.password))
    
    # Create the 'create_table_task' by calling 'PostgresOperator()'
    create_table_task = PostgresOperator(
        task_id          = "create_table",
        postgres_conn_id = "redshift",
        sql              = sql_statements.CREATE_TRIPS_TABLE_SQL
    )

    # Create the 'location_traffic_task' by calling 'PostgresOperator()'
    location_traffic_task = PostgresOperator(
        task_id          = "calculate_location_traffic",
        postgres_conn_id = "redshift",
        sql              = sql_statements.LOCATION_TRAFFIC_SQL
    )

    #===========================================
    #== TASKS INVOCATION SECTION
    #===========================================

    # Call the load task function
    #  Note: 'load_data' represents a discrete invocation of 'load_task()'
    load_data = load_task()

    #========================================================
    #== DEPENDENCY GRAPH
    #========================================================
    create_table_task >> load_data
    load_data         >> location_traffic_task

s3_to_redshift_dag = load_data_to_redshift()
 