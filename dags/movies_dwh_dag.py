from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.movies_plugin import DataQualityOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os

# Define configured variables to connect to AWS S3 and Redshift
movie_s3_config = Variable.get("movie_s3_config", deserialize_json=True)

# Parameters that are reused when submitting spark job to load staging tables
params = {'aws_key': movie_s3_config["awsKey"],
          'aws_secret_key': movie_s3_config["awsSecretKey"],
          'db_user': Variable.get("redshift_db_user"),
          'db_pass': Variable.get("redshift_db_pass"),
          'redshift_conn_string': Variable.get("redshift_conn_string"),
          's3_bucket': movie_s3_config["s3Bucket"],
          's3_key': movie_s3_config["s3Key"]  
         }

# Default settings for DAG
default_args = {
    'owner': 'Alan',
    'depends_on_past': False,
    'start_date': datetime(2020, 5, 24),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

## Define the DAG object
with DAG(dag_id='sparkify_movie_dwh_dag', default_args=default_args,
         description='Load and transform data in Redshift \
                      Data Warehouse with Airflow',
         schedule_interval='@daily') as dag:
    
    start_operator = DummyOperator(task_id='begin-execution', dag=dag)

    # Create tables in movies schema
    create_tables = PostgresOperator(task_id='create-tables', postgres_conn_id="redshift",
                                     sql="sql_scripts/create_tables.sql", dag=dag)
    
    # Load stage_ratings data table
    params['python_script'] = 'load_staging_ratings.py'
    load_staging_ratings = BashOperator(task_id='load-staging-ratings',
                                        bash_command= './bash_scripts/load_staging_table.sh',
                                        params=params,
                                        dag=dag)
    
    # Load stage_movies data table
    params['python_script'] = 'load_staging_movies.py'
    load_staging_movies = BashOperator(task_id='load-staging-movies',
                                       bash_command= './bash_scripts/load_staging_table.sh',
                                       params=params,
                                       dag=dag)

    # Load stage_cpi data table
    params['python_script'] = 'load_staging_cpi.py'
    load_staging_cpi = BashOperator(task_id='load-staging-cpi',
                                    bash_command= './bash_scripts/load_staging_table.sh',
                                    params=params,
                                    dag=dag)
    
    # Run upsert on tables and delete staging tables
    upsert_ratings = PostgresOperator(task_id='upsert-ratings-table', postgres_conn_id="redshift",
                                    sql="sql_scripts/upsert_ratings.sql", dag=dag)

    upsert_movies = PostgresOperator(task_id='upsert-movies-table', postgres_conn_id="redshift",
                                     sql="sql_scripts/upsert_movies.sql", dag=dag)

    upsert_cpi = PostgresOperator(task_id='upsert-staging-cpi', postgres_conn_id="redshift",
                                  sql='sql_scripts/upsert_cpi.sql', dag=dag)
    
    # Check for quality issues in ingested data
    tables = ["movies.movies", "movies.ratings", "movies.movie_genre",
              "movies.genre", "movies.date", "movies.cpi"]
    check_data_quality = DataQualityOperator(task_id='run_data_quality_checks',
                                            redshift_conn_id="redshift",
                                            table_names=tables,
                                            dag=dag)

    # Define data pipeline DAG structure
    start_operator >> create_tables
    create_tables >> [load_staging_ratings, load_staging_movies, load_staging_cpi]
    load_staging_ratings >> upsert_ratings
    load_staging_movies >> upsert_movies
    load_staging_cpi >> upsert_cpi
    [upsert_cpi, upsert_ratings, upsert_movies] >> check_data_quality