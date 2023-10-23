from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from weather_etl import run_weather_etl


# run_weather_etl()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 28),
    'email': ['nimeshaamarasingha@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(days=1)
}

dag = DAG(
    'weather_dag',
    default_args=default_args,
    description='To get singapore weather data!',
    schedule_interval=timedelta(days=1),
)

run_etl = PythonOperator(
    task_id='complete_weather_etl',
    python_callable=run_weather_etl,
    dag=dag, 
)

run_etl