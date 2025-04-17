import os
import sys
import datetime
import logging
from pathlib import Path
import json
import requests
from typing import List, Dict, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

src_path = str(Path(__file__).parent.parent / "src")
if src_path not in sys.path:
    sys.path.append(src_path)

from wat_processor import CommonCrawlWatProcessor


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

def collect_segments(**context) -> None:
    """Collect Common Crawl segments and store them in an Airflow variable."""
    try:
        processor = CommonCrawlWatProcessor()
        
        segment_urls = processor.get_segment_urls()
        
        # NOTE: In a real environment, it will be stored in a more durable storage like S3, or database
        Variable.set(
            "common_crawl_segments",
            {
                "segments": segment_urls,
                "collected_at": datetime.datetime.now().isoformat(),
                "total_segments": len(segment_urls)
            },
            serialize_json=True
        )
        
        logger.info(f"Successfully stored {len(segment_urls)} segments in Airflow variable")
        
    except Exception as e:
        logger.error(f"Error collecting segments: {str(e)}")
        raise

dag = DAG(
    'common_crawl_segment_collector',
    default_args=default_args,
    description='Collects Common Crawl segments and stores them in Airflow variable',
    schedule_interval=datetime.timedelta(days=1),
    start_date=datetime.datetime(2024, 3, 15),
    catchup=False,
    tags=['common_crawl', 'segments'],
)

collect_segments_task = PythonOperator(
    task_id='collect_segments',
    python_callable=collect_segments,
    provide_context=True,
    dag=dag,
)

collect_segments_task 