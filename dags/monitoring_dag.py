from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '/root/airflow')

from utils.pipeline_logger import PipelineLogger
from utils.alert_callbacks import on_success_callback, on_failure_callback

logger = PipelineLogger("monitoring_dag")

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'on_failure_callback': on_failure_callback,
    'on_success_callback': on_success_callback,
}

def check_pipeline_health():
    logger.info("Checking pipeline health...")
    print("Health check passed!")

def log_pipeline_stats():
    logger.info("Logging pipeline statistics...")
    stats = {
        "dag": "monitoring_dag",
        "status": "running",
        "timestamp": str(datetime.now())
    }
    print(f"Pipeline stats logged: {stats}")

def send_alerts():
    logger.info("Alert system check...")
    print("Alert system operational!")

with DAG(
    dag_id='monitoring_dag',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    description='Milestone 3 - Orchestration & Monitoring',
    tags=['milestone3', 'monitoring']
) as dag:

    health_check = PythonOperator(
        task_id='health_check',
        python_callable=check_pipeline_health
    )

    log_stats = PythonOperator(
        task_id='log_stats',
        python_callable=log_pipeline_stats
    )

    alert_check = PythonOperator(
        task_id='alert_check',
        python_callable=send_alerts
    )

    health_check >> log_stats >> alert_check 