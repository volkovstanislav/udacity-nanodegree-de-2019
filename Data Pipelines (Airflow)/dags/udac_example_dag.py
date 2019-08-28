from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
import create_table
import insert_query
import dq_check

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False, #The DAG does not have dependencies on past runs
    'retries': 1, #On failure, the task are retried 3 times
    'retry_delay': timedelta(minutes=5), #Retries happen every 5 minutes
    'catchup' : False, #Catchup is turned off
    'email_on_retry': False, #Do not email on retry
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description="Load and transform data in Redshift with Airflow",
          schedule_interval=None
        )

start_operator = DummyOperator(task_id="Begin_execution",  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    aws_conn_id="aws_credentials",
    redshift_conn_id="redshift",
    create_table_name="public.staging_events",
    s3_path="s3://udacity-dend/log_data",
    create_table_query=create_table.create_staging_events_table,
    copy_from_s3_query=create_table.copy_from_s3,
    json="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    dag=dag,
    aws_conn_id="aws_credentials",
    redshift_conn_id="redshift",
    create_table_name="public.staging_songs",
    s3_path="s3://udacity-dend/song_data/A/A/A/TRAAAAK128F9318786.json",
    create_table_query=create_table.create_staging_songs_table,
    copy_from_s3_query=create_table.copy_from_s3,
    json="auto"
)

load_songplays_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    dag=dag,
    create_table_query=create_table.create_songplays_table,
    redshift_conn_id="redshift",
    create_table_name="songplays",
    insert_table_query=insert_query.songplay_table_insert,
    append_data=True
)

load_user_dimension_table = LoadDimensionOperator(
    task_id="Load_user_dim_table",
    dag=dag,
    create_table_query=create_table.create_users_table,
    redshift_conn_id="redshift",
    create_table_name="users",
    insert_table_query=insert_query.users_table_insert,
    append_data=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id="Load_song_dim_table",
    dag=dag,
    create_table_query=create_table.create_songs_table,
    redshift_conn_id="redshift",
    create_table_name="songs",
    insert_table_query=insert_query.songs_table_insert,
    append_data=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id="Load_artist_dim_table",
    dag=dag,
    create_table_query=create_table.create_artists_table,
    redshift_conn_id="redshift",
    create_table_name="artists",
    insert_table_query=insert_query.artists_table_insert,
    append_data=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    create_table_query=create_table.create_time_table,
    redshift_conn_id="redshift",
    create_table_name="time",
    insert_table_query=insert_query.time_table_insert,
    append_data=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    dq_check=dq_check.dq_checks
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#put dependencies
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
