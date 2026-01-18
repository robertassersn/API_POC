from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Path to your Python script (Docker mounted volume)
SCRIPT_PATH = '/opt/airflow/project/STG/STG_GOOGLE_TRENDS/STG_GOOGLE_TRENDS.py'

with DAG(
    dag_id='google_trends_pipeline',
    default_args=default_args,
    description='Extract Google Trends data and load to PostgreSQL',
    schedule='0 8 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['google_trends', 'etl'],
) as dag:
    
    run_pipeline = BashOperator(
        task_id='run_google_trends_pipeline',
        bash_command=f'python {SCRIPT_PATH}',
    )
