from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers.final_project_sql_statements import SqlQueries

class LoadFactOperator(BaseOperator):
    """
    A class to load the fact table of the star schema that is used to run data analytics.
    
    Attributes:
      redshift_conn_id (str) The Airflow connection id for Redshift.
      table            (str) The name of the target table in Redshift.
    """

    ui_color = '#F98866'

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
                 *args, **kwargs):

        # Call the constructor of parent class and initialize it with inherited attributes
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map inherited parameters
        self.redshift_conn_id = redshift_conn_id
        self.table            = table

    def execute(self, context):

        self.log.info(f"--- [STEP-0] ------- LOAD-FACT-OPERATOR STARTS HERE.\n\tThis operator is going to insert data into the fact table \'{self.table}\' in Redshift.") 
        redshift_hook  = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"--- [STEP-1] ------- ASSESS THAT TABLE EXISTS IN REDSHIFT.")
        result = redshift_hook.get_first("SELECT EXISTS ( SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = '{}' )".format(self.table))
        if result[0] :
            self.log.info(f"The table \'{self.table}\' exists in Redshift.")
        else: 
            raise ValueError(f"[ERROR] The table \'{self.table}\' does not exist in Redshift (result[0]={result[0]}). \n\tPlease create this table before running this DAG. \n\tFYI, you may use the Query-Editor of Redshift to create this table.")
    
        self.log.info(f"--- [STEP-2] ------- CLEARING DATA FROM DESTINATION REDSHIFT TABLE")
        redshift_hook.run("DELETE FROM {}".format(self.table))

        self.log.info(f"--- [STEP-3] ------- INSERTING DATA IN FACT TABLE OF REDSHIFT")
        formatted_sql = "INSERT INTO {} {}".format(self.table, SqlQueries.songplay_table_insert)
        self.log.info(f"Running SQL: {formatted_sql}")
        redshift_hook.run(formatted_sql)
