from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.movies_plugin import DataQualityOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os

movie_s3_config = Variable.get("movie_s3_config", deserialize_json=True)
redshift_conn_string = Variable.get("redshift_conn_string")
db_user = Variable.get("redshift_db_user")
db_pass = Variable.get("redshift_db_pass")
s3_bucket = movie_s3_config["s3Bucket"]
s3_key = movie_s3_config["s3Key"]
aws_key =  movie_s3_config["awsKey"]
aws_secret_key = movie_s3_config["awsSecretKey"]

## Define the DAG object
default_args = {
    'owner': 'Alan',
    'depends_on_past': False,
    'start_date': datetime(2020, 5, 24),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}
with DAG(dag_id='sparkify_movie_dwh_dag', default_args=default_args,
         description='Load and transform data in Redshift \
                      Data Warehouse with Airflow',
         schedule_interval='@daily') as dag:
    
    start_operator = DummyOperator(task_id='begin-execution', dag=dag)
    create_tables = PostgresOperator(task_id='create-tables', postgres_conn_id="redshift",
                                    sql="sql_scripts/create_tables.sql", dag=dag)
    
    load_staging_ratings = BashOperator(task_id='load-staging-ratings',
                                        bash_command= 'spark-submit ' + \
                                        '--driver-class-path $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.43.1067.jar ' + \
                                        '--jars $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.43.1067.jar ' + \
                                        os.getcwd() + '/dags/python_scripts/' + \
                                        'load_staging_ratings.py ' + s3_bucket + ' ' + s3_key + \
                                        ' ' + aws_key + ' ' + aws_secret_key + ' ' + redshift_conn_string + \
                                        ' ' + db_user + ' ' + db_pass + ' ' + \
                                        '--conf "fs.s3a.multipart.size=104857600"' 
                                        , dag=dag)
    
    load_staging_movies = BashOperator(task_id='load-staging-movies',
                                        bash_command= 'spark-submit ' + \
                                        '--driver-class-path $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.43.1067.jar ' + \
                                        '--jars $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.43.1067.jar ' + \
                                        os.getcwd() + '/dags/python_scripts/' + \
                                        'load_staging_movies.py ' + s3_bucket + ' ' + s3_key + \
                                        ' ' + aws_key + ' ' + aws_secret_key + ' ' + redshift_conn_string + \
                                        ' ' + db_user + ' ' + db_pass + ' ' + \
                                        '--conf "fs.s3a.multipart.size=104857600"' 
                                        , dag=dag)
    
    upsert_ratings = PostgresOperator(task_id='upsert-ratings-table', postgres_conn_id="redshift",
                                    sql="sql_scripts/upsert_ratings.sql", dag=dag)

    upsert_movies = PostgresOperator(task_id='upsert-movies-table', postgres_conn_id="redshift",
                                     sql="sql_scripts/upsert_movies.sql", dag=dag)

    upsert_cpi = PostgresOperator(task_id='upsert-staging-cpi', postgres_conn_id="redshift",
                                  sql='sql_scripts/upsert_cpi.sql',
                                  params={
                                    'schema': 'movies',
                                    'table': 'stage_cpi',
                                    's3_bucket': s3_bucket,
                                    's3_key': s3_key,
                                    'file_name': 'consumer_price_index.csv'
                                    'access_key':  aws_key,
                                    'secret_key': aws_secret_key
                                  }, dag=dag)
    
    check_data_quality = DataQualityOperator(task_id='run_data_quality_checks',
                                            redshift_conn_id="redshift",
                                            table_names=["movies.movies",
                                                         "movies.ratings",
                                                         "movies.movie_genre",
                                                         "movies.genre",
                                                         "movies.date",
                                                         "movies.cpi"],
                                            dag=dag)

    start_operator >> create_tables
    create_tables >> [load_staging_ratings, load_staging_movies, upsert_cpi]
    load_staging_ratings >> upsert_ratings
    load_staging_movies >> upsert_movies
    [upsert_cpi, upsert_ratings, upsert_movies] >> check_data_quality