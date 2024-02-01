from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from getDataHdfs import ingest_all_data
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


def hello():
    print("hello world")

# Define default_args dictionary to specify the DAG's default parameters
default_args = {
    'owner': 'jenifer',
    'depends_on_past': False,
    'start_date': datetime(2024, 0o1, 0o21),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate a DAG with the above parameters
dag = DAG(
    'jeniferhdfs',
    default_args=default_args,
    schedule_interval='@daily',  
    catchup=False,
)

submit_spark_job = SparkSubmitOperator(
    task_id='submit_spark_job',
    application='hdfs://localhost:9000/user/project/datalake/main-scala-mnm_2.12-1.0.jar',
    conn_id='spark_default',  
    java_class='main.scala.mnm.MnMcount',
    dag=dag,
)


task_popular_movies = PythonOperator(
    task_id='popular_movies_task',
    python_callable = ingest_all_data,
    provide_context=True,
    dag=dag,
)



# Set up task dependencies
task_popular_movies >> submit_spark_job
