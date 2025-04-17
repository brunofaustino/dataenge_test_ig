from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import logging
import json
import os

from src.data_processor 
from src.website_categorizer import WebsiteCategorizer

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def download_segments(**context):
    """Download Common Crawl segments."""
    processor = CommonCrawlProcessor()
    segments = processor.download_latest_segments(num_segments=3)
    return segments

def process_segments(**context):
    """Process downloaded segments and extract links."""
    ti = context['task_instance']
    segments = ti.xcom_pull(task_ids='download_segments')
    
    processor = CommonCrawlProcessor()
    result_df = processor.process_segments(segments)
    
    # Save to XCom for next task
    return result_df.to_dict('records')

def categorize_websites(**context):
    """Categorize websites using the WebsiteCategorizer."""
    ti = context['task_instance']
    links_data = ti.xcom_pull(task_ids='process_segments')
    
    categorizer = WebsiteCategorizer()
    categorized_data = []
    
    for link in links_data:
        if link['target_domain']:
            result = categorizer.categorize_website(link['target_url'])
            link.update(result)
        categorized_data.append(link)
    
    return categorized_data

def compute_metrics(**context):
    """Compute metrics from the processed data."""
    ti = context['task_instance']
    categorized_data = ti.xcom_pull(task_ids='categorize_websites')
    
    processor = CommonCrawlProcessor()
    metrics = processor.compute_metrics(categorized_data)
    
    return metrics

def save_to_postgres(**context):
    """Save processed data to PostgreSQL."""
    ti = context['task_instance']
    categorized_data = ti.xcom_pull(task_ids='categorize_websites')
    metrics = ti.xcom_pull(task_ids='compute_metrics')
    
    processor = CommonCrawlProcessor()
    processor.save_to_postgres(categorized_data, metrics)

def save_to_arrow(**context):
    """Save processed data to Arrow/Parquet format."""
    ti = context['task_instance']
    categorized_data = ti.xcom_pull(task_ids='categorize_websites')
    
    processor = CommonCrawlProcessor()
    processor.save_to_arrow(categorized_data)

with DAG(
    'common_crawl_processor',
    default_args=default_args,
    description='Process Common Crawl data and categorize websites',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['common_crawl', 'web_categorization'],
) as dag:

    download_task = PythonOperator(
        task_id='download_segments',
        python_callable=download_segments,
    )

    process_task = PythonOperator(
        task_id='process_segments',
        python_callable=process_segments,
    )

    categorize_task = PythonOperator(
        task_id='categorize_websites',
        python_callable=categorize_websites,
    )

    metrics_task = PythonOperator(
        task_id='compute_metrics',
        python_callable=compute_metrics,
    )

    save_postgres_task = PythonOperator(
        task_id='save_to_postgres',
        python_callable=save_to_postgres,
    )

    save_arrow_task = PythonOperator(
        task_id='save_to_arrow',
        python_callable=save_to_arrow,
    )

    # Define task dependencies
    download_task >> process_task >> categorize_task >> [metrics_task, save_postgres_task, save_arrow_task] 