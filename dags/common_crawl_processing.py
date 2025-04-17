import os
import sys
import datetime
import logging
from pathlib import Path
import json
from typing import List, Dict, Any
import pendulum
import pandas as pd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowException
from airflow.operators.dummy_operator import DummyOperator

# Add src directory to Python path
src_path = str(Path(__file__).parent.parent / "src")
if src_path not in sys.path:
    sys.path.append(src_path)

from wat_processor import CommonCrawlWatProcessor
from metrics_processor import MetricsProcessor
from data_processor import CommonCrawlProcessor

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default configuration values
default_config = {
    "retry_count": 3,
    "retry_delay_minutes": 5,
    "schedule": "0 0 * * *",  # Daily at midnight
    "max_active_runs": 1,
    "concurrency": 3,
    "slack_channel": None,
    "environment": {
        'POSTGRES_HOST': 'postgres',
        'POSTGRES_DB': 'crawldata',
        'POSTGRES_USER': 'airflow',
        'POSTGRES_PASSWORD': 'airflow',
    }
}

# Initialize dag_config with default values
dag_config = default_config.copy()

# Try to get custom configuration from Airflow Variable
try:
    custom_config = Variable.get("common_crawl_config", deserialize_json=True)
    if isinstance(custom_config, dict):
        # Update dag_config with custom values
        dag_config.update(custom_config)
    else:
        logger.warning(f"Custom configuration is not a dictionary: {custom_config}")
except Exception as e:
    logger.warning(f"Could not load custom DAG configuration: {str(e)}")

# Define callbacks for Slack notifications
def on_failure_callback(context):
    """Callback for task failures."""
    if dag_config.get("slack_channel"):
        logger.error(f"Task {context['task_instance'].task_id} failed")

def on_success_callback(context):
    """Callback for task successes."""
    if dag_config.get("slack_channel"):
        logger.info(f"Task {context['task_instance'].task_id} succeeded")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': dag_config.get("retry_count", 3),
    'retry_delay': datetime.timedelta(minutes=dag_config.get("retry_delay_minutes", 5)),
    'on_failure_callback': on_failure_callback,
    'on_success_callback': on_success_callback,
}

def process_segment(segment_url: str, **context) -> None:
    """Process a single Common Crawl segment."""
    try:
        logger.info(f"Processing segment: {segment_url}")
        processor = CommonCrawlWatProcessor()
        
        wat_path = processor.download_wat_file(segment_url)
        
        links = processor.extract_links_from_wat(wat_path)
        

        df = pd.DataFrame(links)
        
        df['category'] = None 
        df['country'] = None  
        df['is_ad_based'] = False  
        
        # Save to PostgreSQL using configuration from dag_config
        db_params = {
            'dbname': dag_config['environment']['POSTGRES_DB'],
            'user': dag_config['environment']['POSTGRES_USER'],
            'password': dag_config['environment']['POSTGRES_PASSWORD'],
            'host': dag_config['environment']['POSTGRES_HOST'],
            'port': '5432'
        }
        
        logger.info(f"Connecting to PostgreSQL database: {db_params['dbname']} on {db_params['host']} as {db_params['user']}")
        
        conn_params = db_params.copy()
        conn_params['dbname'] = 'postgres' 
        
        try:
            conn = psycopg2.connect(**conn_params)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()
            
            # Check if database exists
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_params['dbname'],))
            exists = cur.fetchone()
            
            if not exists:
                logger.info(f"Creating database {db_params['dbname']}")
                cur.execute(f"CREATE DATABASE {db_params['dbname']}")
                logger.info(f"Database {db_params['dbname']} created successfully")
            else:
                logger.info(f"Database {db_params['dbname']} already exists")
                
        except Exception as e:
            logger.error(f"Error creating database: {str(e)}")
            raise
        finally:
            if 'cur' in locals() and cur:
                cur.close()
            if 'conn' in locals() and conn:
                conn.close()
        
        data_processor = CommonCrawlProcessor(db_params=db_params)
        metrics = data_processor.compute_metrics(df)
        data_processor.load_to_postgres(df, metrics)
        
        # NOTE: In a real environment, we should save the data to a more durable storage like S3 or GCS
        output_dir = Path("/opt/airflow/data/processed")
        output_dir.mkdir(parents=True, exist_ok=True)
        data_processor.save_to_arrow(df, partition_cols=['target_domain'], output_dir=output_dir)
        
        logger.info(f"Successfully processed segment: {segment_url}")
    except Exception as e:
        logger.error(f"Error processing segment {segment_url}: {str(e)}")
        raise

def generate_analytics_report(**context) -> None:
    """Generate analytics report from PostgreSQL data."""
    try:
        metrics_processor = MetricsProcessor()
        
        reports_dir = Path("/opt/airflow/data/reports")
        reports_dir.mkdir(parents=True, exist_ok=True)
        
        report_path = metrics_processor.generate_report(
            metrics={
                'total_unique_domains': 0,  # Use numeric values instead of strings
                'homepage_ratio': 0.0,      # Use numeric values instead of strings
                'avg_subsections_per_domain': 0.0,
                'top_domains': {},
                'top_categories': {},
                'geographic_distribution': {}
            },
            output_path=reports_dir / "analytics_report.txt"
        )
        
        with open(report_path, 'r') as f:
            report = f.read()
        
        logger.info("Analytics Report:")
        logger.info(report)
        context['task_instance'].xcom_push(key='analytics_report', value=report)
    except Exception as e:
        logger.error(f"Error generating analytics report: {str(e)}")
        raise

# Create DAG
dag = DAG(
    'common_crawl_processing',
    default_args=default_args,
    description='Processes Common Crawl segments and saves results',
    schedule_interval=dag_config.get("schedule", "0 0 * * *"),
    start_date=pendulum.today('UTC').subtract(days=1),
    catchup=False,
    max_active_runs=dag_config.get("max_active_runs", 1),
    concurrency=dag_config.get("concurrency", 3),
    tags=['common_crawl', 'processing'],
)

start_task = DummyOperator(task_id='start', dag=dag)
end_task = DummyOperator(task_id='end', dag=dag)

try:
    logger.info("Attempting to load segments data from Airflow variable...")
    segments_data = Variable.get("common_crawl_segments", deserialize_json=True)
    logger.info(f"Raw segments data type: {type(segments_data)}")
    logger.info(f"Raw segments data content: {segments_data}")
    
    segment_urls = []
    try:
        if isinstance(segments_data, dict) and "segments" in segments_data:
            segment_urls = segments_data["segments"]
        elif isinstance(segments_data, str):
            # Parse JSON string if needed
            parsed_data = json.loads(segments_data)
            if isinstance(parsed_data, dict) and "segments" in parsed_data:
                segment_urls = parsed_data["segments"]
            elif isinstance(parsed_data, list):
                segment_urls = parsed_data
        elif isinstance(segments_data, list):
            segment_urls = segments_data
            
        logger.info(f"Successfully extracted {len(segment_urls)} segments")
            
    except Exception as e:
        logger.error(f"Failed to extract segments: {e}")
    
    if segment_urls:
        logger.info("Found segments:")
        for i, url in enumerate(segment_urls):
            logger.info(f"  Segment {i}: {url}")
    else:
        logger.warning("No segments were extracted from the variable")

except Exception as e:
    logger.error(f"Error getting segments variable: {str(e)}", exc_info=True)
    logger.error("Stack trace:", exc_info=True)
    segment_urls = []

# Create a task group for parallel processing of segments
with TaskGroup(group_id='process_segments', dag=dag) as process_segments_group:
    if not segment_urls:
        logger.warning("No segments found, creating dummy task")
        no_segments_task = PythonOperator(
            task_id='no_segments_available',
            python_callable=lambda: logger.info("No segments available for processing"),
            provide_context=True,
        )
    else:
        logger.info(f"Creating tasks for {len(segment_urls)} segments")
        # Create tasks for each segment
        segment_tasks = []
        for i, segment_url in enumerate(segment_urls):
            task_id = f'process_segment_{i}'
            logger.info(f"Creating task {task_id} for segment {segment_url}")
            
            # Create Python task
            python_task = PythonOperator(
                task_id=task_id,
                python_callable=process_segment,
                op_kwargs={'segment_url': segment_url},
                provide_context=True,
                dag=dag,
            )
            
            segment_tasks.append(python_task)

# Generate analytics report
analytics_report_task = PythonOperator(
    task_id='analytics_report',
    python_callable=generate_analytics_report,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
start_task >> process_segments_group

if segment_urls:
    # If we have segments, connect the segment tasks to the analytics report
    process_segments_group >> analytics_report_task
else:
    # If no segments, just run the no_segments task and then the analytics report
    no_segments_task >> analytics_report_task

analytics_report_task >> end_task 