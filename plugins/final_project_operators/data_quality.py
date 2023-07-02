from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    A class to execute and measure some data quality tests on the dataset. 
    This operateor receives one or more SQL based test cases along with the
    expected results and execute the tests. 
    For each the test, the result and expected result are checked, and if
    there is no match, the operator will raise an exception,  and the task
    will be retried.

    For example one test could be a SQL statement that checks if certain
    column contains NULL values by counting all the rows that have NULL
    in the column. We do not want to have any NULLs so expected result
    would be 0 and the test would compare the SQL statement's outcome to
    the expected result.

    Attributes:
      task_id          (str)  A unique, meaningful id for the task.
      redshift_conn_id (str)  The Airflow connection id for 'redshift'.
      dq_checks        (list) A list of dictionaries, where each dictionary
                               represents a data quality checks.

    Example:
        run_quality_checks = DataQualityOperator(
            task_id           = 'Run_data_quality_checks',
            redshift_conn_id  = 'redshift',
            dq_checks         = [
                                    { 
                                        'sql_test_case'  : 'SELECT COUNT(*) FROM user WHERE userid IS NULL', 
                                        'expected_result': 0 
                                    },
                                    { 
                                        'sql_test_case'  : 'SELECT COUNT(DISTINCT "level") FROM public.songplay', 
                                        'expected_result': 2 
                                    }
                                ]
            )
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,  
                # Operator parameters
                redshift_conn_id="",
                dq_checks="",
                *args, **kwargs):

        # Call the constructor of parent class and initialize it with inherited attributes
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map inherited parameters
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks        = dq_checks

    def execute(self, context):
        default_args = self.dag.default_args
        self.log.info(f"--- [STEP-0] ------- DATA QUALITY CHECK OPERATOR STARTS HERE.\n\tThis operator is going to execute and measure some data quality tests on the dataset.") 
        redshift_hook  = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"--- [STEP-1] ------- RUN THE LIST OF QUALITY TESTS.")
        for index, dq_check in enumerate(self.dq_checks):
            self.log.info(f"NOW RUNNING TC[{index}] --> \'{dq_check['sql_test_case']}\' --> EXPECTING RESULT --> \'{dq_check['expected_result']}\'")
            formatted_sql = "{}".format(dq_check['sql_test_case'])
            self.log.info(f"Running SQL: {formatted_sql}")
            records = redshift_hook.get_records(formatted_sql)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"DATA QUALITY CHECK FAILED: SQL check returned no results")
            if records[0][0] != dq_check['expected_result']:
                raise ValueError(f"DATA QUALITY CHECK FAILED: TC[{index}] --> EXPECTED \'{dq_check['expected_result']}\' -- RECEIVED \'{records[0][0]}\' ")
            self.log.info(f"DATA QUALITY CHECK TC[{index}] --> OK")
