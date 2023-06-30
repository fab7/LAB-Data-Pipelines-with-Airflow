from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    A class to load a JSON formatted file from S3 to Amazon Redshift.
    This operator uses a SQL COPY statement to dynamically stage the data
    from S3 to Redshift.

    Attributes:
      task_id          (str) A unique, meaningful id for the task.
      table            (str) The name of the target table in Redshift.
      redshift_conn_id (str) The Airflow connection id for 'redshift'.
      aws_conn_id      (str) The Airflow connection id for 'aws'.
      s3_bucket        (str) The name of the S3 bucket to read from.
      s3_key           (str) The path to the data on S3.

    Templated fields:

    TODO:
      The operator's parameters should specify where in S3 the file is loaded
        and what is the target table. The parameters should be used to 
        distinguish between JSON file. 
      Another important requirement of the stage operator is containing a 
      templated field that allows it to load timestamped files from S3 based
      on the execution time and run backfills.
    """

    ui_color = '#358140'

    # List of template fields for this operator. 
    #  Note: Template fields are placeholders in the operator's parameters 
    #   that can be templated. Defining template fields makes this operators
    #   more flexible and reusable as these parameters aer dynamically set. 
    template_fields = ("s3_bucket", "s3_key", "table")

    # Copy task to dynamically stage the data from S3 to Redshift.
    COPY_SQL = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON 'auto'
      """
     
    @apply_defaults
    def __init__(self,
                 # Operator parameters
                 redshift_conn_id="",
                 table="",
                 aws_conn_id="",
                 s3_bucket="",
                 s3_key="",
                 year="2018",
                 month="01",
                 *args, **kwargs):

        # Call the constructor of parent class and initialize it with inherited attributes
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map inherited parameters
        self.redshift_conn_id = redshift_conn_id
        self.table            = table
        self.aws_conn_id      = aws_conn_id
        self.s3_bucket        = s3_bucket
        self.s3_key           = s3_key
        self.year             = year
        self.month            = month

      
    def execute(self, context):
      
      self.log.info(f"--- [STEP-0] ------- STAGE-TO-REDSHIFT-OPERATOR STARTS HERE.\n\tThis operator is going to copy the data from \'s3://{self.s3_bucket}/{self.s3_key}\' to the Redshift table \'{self.table}\'.") 
      aws_hook  = AwsHook(self.aws_conn_id)
      aws_cred  = aws_hook.get_credentials()
      redshift_hook  = PostgresHook(postgres_conn_id=self.redshift_conn_id)

      # Check if table exists
      # This query will return true if the table exists and false if it does not.
      self.log.info(f"--- [STEP-1] ------- ASSESS THAT TABLE EXISTS IN REDSHIFT.")
      result = redshift_hook.get_first("SELECT EXISTS ( SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = '{}' )".format(self.table))
      # Access the result
      if result[0] :
         self.log.info(f"The table \'{self.table}\' exists in Redshift.")
      else: 
        raise ValueError(f"[ERROR] The table \'{self.table}\' does not exist in Redshift (result[0]={result[0]}). Please create this table before running this DAG. FYI, you may use the Query-Editor of Redshift to create this table.")
    
      self.log.info("--- [STEP-2] ------- CLEARING DATA FROM DESTINATION REDSHIFT TABLE")
      redshift_hook.run("DELETE FROM {}".format(self.table))

      self.log.info("--- [STEP-3] ------- COPYING DATA FROM S3 TO REDSHIFT")

      # Distinguish between the two types of JSON files to load.
      #   'log-data/2018/11/2018-11-01-events.json'
      #   'song-data/A/A/A/TRAAAAK128F9318786.json'
      if self.s3_key == 'log-data':
          s3_path = "s3://{}/{}/{}/{}/".format(self.s3_bucket, self.s3_key, self.year, self.month)
          # FIXME s3_path = "s3://{}/{}/{}/{}/".format(self.s3_bucket, self.s3_key, self.year, self.month)
      elif self.s3_key == 'song-data':
          s3_path = "s3://{}/{}/A/A/A/".format(self.s3_bucket, self.s3_key)
          # FIXME s3_path = "s3://{}/{}/".format(self.s3_bucket, self.s3_key) 
      else:
        raise ValueError(f"[ERROR] The S3 key \'{self.s3_key}\' does not match ['log-data'|'song-data']. Please use an appropriate \'s3_key\' when calling this operator.")
     
      formatted_sql = StageToRedshiftOperator.COPY_SQL.format(
                        self.table,
                        s3_path,
                        aws_cred.access_key,
                        aws_cred.secret_key
                      )
      self.log.info(f"Running SQL: {formatted_sql}")
      redshift_hook.run(formatted_sql)
