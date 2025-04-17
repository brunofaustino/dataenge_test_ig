import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Add project root to Python path to allow importing our modules
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.metrics_processor import CommonCrawlProcessor

def process_common_crawl_data():
    """Process Common Crawl WAT files and store metrics in PostgreSQL."""
    processor = CommonCrawlProcessor()
    processor.process_wat_files()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'common_crawl_processing',
    default_args=default_args,
    description='Process Common Crawl WAT files and store metrics',
    schedule_interval='@daily',
    start_date=datetime(2024, 3, 15),
    catchup=False,
    tags=['common_crawl', 'web_metrics'],
) as dag:

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
    )

    process_data = PythonOperator(
        task_id='process_data',
        python_callable=process_common_crawl_data,
    )

    create_tables >> process_data 