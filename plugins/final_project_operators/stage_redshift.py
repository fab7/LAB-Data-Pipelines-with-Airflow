from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    A class to load a JSON formatted file from S3 to Amazon Redshift.
    This Airflow operator creates and runs a SQL COPY statement based on the
     parameters provided. 

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
    
    # FIXME - template_fields = ("s3_bucket", "s3_key", "table", )
    template_fields = ("s3_bucket")

    # This operator uses a SQL COPY statement to dynamically stage the data
    # from S3 to Redshift. The copy task uses the provided template fields.
    COPY_SQL = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON 'auto'
      """
     
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 s3_bucket="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.s3_bucket          = s3_bucket

    def execute(self, context):
        self.log.info('<INFO> StageToRedshiftOperator starts here')
        self.log.info(f"<INFO> This operator is going to copy the data TODO from the S3 bucket \'{self.s3_bucket}\' to the Redshift table \'TODO\'.") 
     

    # @apply_defaults
    # def __init__(self,
    #              # Operator parameters
    #              redshift_conn_id="redshift",
    #              aws_credentials_id="aws_credentials",
    #              table="",
    #              s3_bucket="",
    #              s3_key="",
    #              #delimiter=",",
    #              #ignore_headers=1,
    #              *args, **kwargs):

        # # Call constructor of parent class and initialize it with inherited attributes
        # super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # # Map inherited parameters
        # self.redshift_conn_id   = redshift_conn_id
        # self.aws_credentials_id = aws_credentials_id
        # self.table              = table
        # self.s3_bucket          = s3_bucket
        # self.s3_key             = s3_key
        # #self.delimiter          = delimiter
        # #self.ignore_headers     = ignore_headers
        

    # def execute(self, context):
    #     #OBSOLETE self.log.info('StageToRedshiftOperator not implemented yet')
    #     aws_hook    = AwsHook(self.aws_credentials_id)
    #     credentials = aws_hook.get_credentials()
    #     redshift    = PostgresHook(postgres_conn_id=self.redshift_conn_id)

    #     self.log.info("Clearing data from destination Redshift table")
    #     redshift.run("DELETE FROM {}".format(self.table))

    #     self.log.info("Copying data from S3 to Redshift")
    #     rendered_key = self.s3_key.format(**context)
    #     s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
    #     formatted_sql = S3ToRedshiftOperator.COPY_SQL.format(
    #         self.table,
    #         s3_path,
    #         credentials.access_key,
    #         credentials.secret_key
    #       #    self.ignore_headers,
    #       #    self.delimiter
    #       )
    #     self.log.info(f"Running SQL: {formatted_sql}")
    #     redshift.run(formatted_sql)
