from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '/root/airflow')

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_etl():
    from dags.master_etl_dag import run_pipeline
    run_pipeline('/root/airflow/data/sample_customers.csv')

with DAG(
    dag_id='enterprise_etl_pipeline',
    default_args=default_args,
    description='Enterprise Data Cleaning & ETL Pipeline',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'production', 'milestone1'],
) as dag:

    extract_clean_transform = PythonOperator(
        task_id='extract_clean_transform_load',
        python_callable=run_etl,
    )