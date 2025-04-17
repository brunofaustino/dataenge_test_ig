from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import json
import os
from src.data_processor import process_segment
import gzip
import io

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_latest_segments():
    """
    Get the latest Common Crawl segments from the index.
    """
    index_url = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-50/wat.paths.gz"
    response = requests.get(index_url)
    response.raise_for_status()
    
    # Decompress the gzipped content
    content = gzip.decompress(response.content).decode('utf-8')
    
    segments = []
    for line in content.split('\n'):
        if line.strip():
            segments.append(f"https://data.commoncrawl.org/{line.strip()}")
    
    return segments[:5]  # Process only the first 5 segments for testing

def process_segment_task(**context):
    """
    Process a single Common Crawl segment.
    """
    segment_url = context['segment_url']
    output_dir = "/opt/airflow/data"
    
    # Ensure output directories exist
    os.makedirs(f"{output_dir}/raw", exist_ok=True)
    os.makedirs(f"{output_dir}/final", exist_ok=True)
    
    # Database connection parameters
    db_params = {
        'dbname': 'crawldata',
        'user': 'dataengineer',
        'password': 'dataengineer',
        'host': 'postgres',
        'port': '5432'
    }
    
    # Process only the first 50MB of each segment
    process_segment(segment_url, output_dir, db_params, max_size_mb=50)

with DAG(
    'common_crawl_analysis',
    default_args=default_args,
    description='Process Common Crawl data to extract external links',
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:
    
    def create_segment_tasks():
        segments = get_latest_segments()
        tasks = []
        
        for i, segment_url in enumerate(segments):
            task = PythonOperator(
                task_id=f'process_segment_{i}',
                python_callable=process_segment_task,
                op_kwargs={'segment_url': segment_url},
                dag=dag
            )
            tasks.append(task)
        
        return tasks
    
    # Create tasks dynamically
    segment_tasks = create_segment_tasks()
    
    # Set task dependencies if needed
    # For now, we'll process segments in parallel 