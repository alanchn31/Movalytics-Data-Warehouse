from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os

# docker run -d -p 8080:8080 -v //c/Users/Work-pc/Desktop/capstone/dags://usr/local/airflow/dags  alanchn31-capstone-udacity-de-nd

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
    
    start_operator = DummyOperator(task_id='begin_execution', dag=dag)
    printVersion = BashOperator(task_id='check-version',
                                bash_command='ls $SPARK_HOME/jars',
                                dag=dag)
    downloadData = BashOperator(task_id='download-data',
                                bash_command= 'spark-submit ' + \
                                '--driver-class-path $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.43.1067.jar ' + \
                                '--jars $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.43.1067.jar ' + \
                                os.getcwd() + '/dags/python_scripts/' + \
                                'load_ratings.py ' + s3_bucket + ' ' + s3_key + \
                                ' ' + aws_key + ' ' + aws_secret_key + ' ' + redshift_conn_string + \
                                ' ' + db_user + ' ' + db_pass + ' ' + '--conf "fs.s3a.multipart.size=104857600"' #spark.hadoop.fs.s3a
                                , dag=dag)
    start_operator >> printVersion >> downloadData