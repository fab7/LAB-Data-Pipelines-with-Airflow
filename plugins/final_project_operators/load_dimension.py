from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers.final_project_sql_statements import SqlQueries

class LoadDimensionOperator(BaseOperator):
    """
    A class to load the dimension tables of the star schema that is used to run data analytics.
    
    Attributes:
      redshift_conn_id (str) The Airflow connection id for Redshift.
      table            (str) The name of the target table in Redshift.
      insert_mode      (str) The pattern to use for inserting data into the table.
                             The insert mode can be ['append-only'|'delete-load']. 
    """

    ui_color = '#80BD9E'

    # List of template fields for this operator. 
    #  Note: Template fields are placeholders in the operator's parameters 
    #   that can be templated. Defining template fields makes this operators
    #   more flexible and reusable as these parameters aer dynamically set. 
    template_fields = ("table")

    @apply_defaults
    def __init__(self,
                 # Operator parameters
                 redshift_conn_id="",
                 table="",
                 insert_mode="delete-load",
                 *args, **kwargs):

        # Call the constructor of parent class and initialize it with inherited attributes
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map inherited parameters
        self.redshift_conn_id = redshift_conn_id
        self.table            = table
        self.insert_mode      = insert_mode


    def execute(self, context):

        self.log.info(f"--- [STEP-0] ------- LOAD-DIMENSION-OPERATOR STARTS HERE.\n\tThis operator is going to insert data into the dim table \'{self.table}\' in Redshift.") 
        redshift_hook  = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"--- [STEP-1] ------- ASSESS THAT TABLE EXISTS IN REDSHIFT.")
        result = redshift_hook.get_first("SELECT EXISTS ( SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = '{}' )".format(self.table))
        if result[0] :
            self.log.info(f"The table \'{self.table}\' exists in Redshift.")
        else: 
            raise ValueError(f"[ERROR] The table \'{self.table}\' does not exist in Redshift (result[0]={result[0]}). \n\tPlease create this table before running this DAG. \n\tFYI, you may use the Query-Editor of Redshift to create this table.")
    
        # FYI - The truncate-insert pattern (a.k.a delete-load pattern) is a 
        #  common data loading technique used in data engineering. It involves
        #  truncating (deleting) the existing data in a target table and then
        #  inserting new data into the table. This pattern is often used when
        #  you want to replace the entire contents of a table with new data.
        #  By following the truncate-insert pattern, you ensure that the target
        #  table is always refreshed with the latest data. This often done for
        #  dimention tables and is particularly useful when you have periodic
        #  data updates or when you want to replace the entire contents of a 
        #  table with new data.
        if self.insert_mode == "delete-load":
            self.log.info(f"--- [STEP-2] ------- SWITCHING TO TRUNCATE-INSERT PATTERN LOADING MODE")
            # Warning: 'user' is a reserved keyword in Redshift, so we must be
            #  extra cautious when using it as a table name. Because one of 
            #  our table is named 'user', we must always enclose it in double
            #  quotation marks (") to avoid any conflicts with the reserved
            #  'user' keyword.
            redshift_hook.run("DELETE FROM \"{}\" ".format(self.table))
        elif self.insert_mode == "append-only":
            self.log.info(f"--- [STEP-2] ------- SWITCHING TO APPENDING-ONLY PATTERN LOADING MODE")
        else:
             raise ValueError(f"[ERROR] The insert pattern mode \'{self.Insert_mode}\' does not exist or is not supported.")

        self.log.info(f"--- [STEP-3] ------- INSERTING DATA IN DIMENSION TABLE OF REDSHIFT")
        table_to_insert = self.table + "_table_insert"
        dim_query = getattr(SqlQueries, table_to_insert)     
        formatted_sql = "INSERT INTO \"{}\" {} ".format(self.table, dim_query)
        self.log.info(f"Running SQL: {formatted_sql}")
        redshift_hook.run(formatted_sql)
