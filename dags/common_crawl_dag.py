from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import sys
import os

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.main import process_common_crawl_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'common_crawl_processing',
    default_args=default_args,
    description='Process Common Crawl data and load into PostgreSQL',
    schedule_interval='@daily',
    catchup=False
)

# SQL to create required tables
create_tables = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id='postgres_default',
    sql="""
    CREATE TABLE IF NOT EXISTS website_metrics (
        id SERIAL PRIMARY KEY,
        metric_name VARCHAR(255),
        metric_value JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    dag=dag
)

# Task to process Common Crawl data
process_data = PythonOperator(
    task_id='process_data',
    python_callable=process_common_crawl_data,
    dag=dag
)

# Set task dependencies
create_tables >> process_data 