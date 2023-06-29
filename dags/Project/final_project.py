from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements


default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    # TODO - Retries,
    # TODO - Retry_delay
    # TODO - Catchup   
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *' # TODO - Once an hour
)
def final_project():

    #===========================================
    #== TASKS INVOCATION SECTION
    #===========================================
    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        s3_bucket         = 'fab-se4s-bucket',
    )

    # stage_events_to_redshift = StageToRedshiftOperator(
    #     task_id           = 'Stage_events',
    #     table             = 'staging_events',
    #     redshift_conn_id  = 'redshift',
    #     aws_conn_id       = 'aws_credentials',
    #     s3_bucket         = 'fab-se4s-bucket',
    #     s3_key            = 'log_data/2018/11/2018-11-01-events.json'
    #     )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        s3_bucket         = 'fab-se4s-bucket',
    )

    # stage_songs_to_redshift = StageToRedshiftOperator(
    #     task_id           = 'Stage_songs',
    #     table             = 'staging_songs',
    #     redshift_conn_id  = 'redshift',
    #     aws_conn_id       = 'aws_credentials',
    #     s3_bucket         = 'fab-se4s-bucket',
    #     s3_key            = 'song_data/A/A/A/TRAAAAK128F9318786.json'
    #     )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
    )

    end_operator = DummyOperator(task_id='End_execution')

    #========================================================
    #== DEPENDENCY GRAPH
    #========================================================
    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift

    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift  >> load_songplays_table

    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table

    load_user_dimension_table   >> run_quality_checks
    load_song_dimension_table   >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table   >> run_quality_checks

    run_quality_checks >> end_operator


final_project_dag = final_project()