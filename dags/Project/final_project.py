from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator

from helpers.final_project_sql_statements import SqlQueries


default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    # TODO - Retries,
    # TODO - Retry_delay
    # TODO - Catchup OFF  
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    #start_date=pendulum.datetime(2018, 1, 1, 0, 0, 0, 0),
    #end_date=pendulum.datetime(2018, 2, 1, 0, 0, 0, 0),
    schedule_interval='0 * * * *' # TODO - Once an hour
)
def final_project():

    #===========================================
    #== TASKS INVOCATION SECTION
    #===========================================
    start_operator = DummyOperator(task_id='Begin_execution')
      
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id           = 'Stage_events',
        redshift_conn_id  = 'redshift',
        table             = 'staging_events',
        aws_conn_id       = 'aws_credentials',
        s3_bucket         = 'fab-se4s-bucket',      # 'udacity-dend',
        s3_key            = 'log-data',
        year              = '2018',
        month             = '11'
        )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id           = 'Stage_songs',
        redshift_conn_id  = 'redshift',
        table             = 'staging_songs',
        aws_conn_id       = 'aws_credentials',
        s3_bucket         = 'fab-se4s-bucket',      # 'udacity-dend',
        s3_key            = 'song-data'
        )

    load_songplay_table = LoadFactOperator(
        task_id           = 'Load_songplay_fact_table',
        redshift_conn_id  = 'redshift',
        table             = 'songplay'
        )

    load_user_dimension_table = LoadDimensionOperator(
        task_id           = 'Load_user_dim_table',
        redshift_conn_id  = 'redshift',
        table             = 'user'
        )

    load_song_dimension_table = LoadDimensionOperator(
        task_id           = 'Load_song_dim_table',
        redshift_conn_id  = 'redshift',
        table             = 'song'
        )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id           = 'Load_artist_dim_table',
        redshift_conn_id  = 'redshift',
        table             = 'artist'      
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id           = 'Load_time_dim_table',
        redshift_conn_id  = 'redshift',
        table             = 'time'
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

    stage_events_to_redshift >> load_songplay_table
    stage_songs_to_redshift  >> load_songplay_table

    load_songplay_table >> load_user_dimension_table
    load_songplay_table >> load_song_dimension_table
    load_songplay_table >> load_artist_dimension_table
    load_songplay_table >> load_time_dimension_table

    load_user_dimension_table   >> run_quality_checks
    load_song_dimension_table   >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table   >> run_quality_checks

    run_quality_checks >> end_operator


final_project_dag = final_project()
