from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from getDataHdfs import ingest_all_data
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


# Define default_args dictionary to specify the DAG's default parameters
default_args = {
    'owner': 'jenifer',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='jeniferhdfs',
    default_args=default_args,
    schedule_interval='@daily',
    start_date= datetime(2024, 0o1, 0o1),  
) as dag:
    pass
    submit_spark_job = SparkSubmitOperator(
        task_id='submit_spark_job',
        application='/home/ubuntu/airflow/mnmcount/scala/target/scala-2.12/main-scala-mnm_2.12-1.0.jar',
        conn_id='spark_default',  
        java_class='main.scala.mnm.MnMcount',
    )


    task_popular_movies = PythonOperator(
        task_id='popular_movies_task',
        python_callable = ingest_all_data,
        provide_context=True,
    )
    
    # Set up task dependencies
    task_popular_movies >> submit_spark_job

#pip install apache-airflow-providers-apache-spark
