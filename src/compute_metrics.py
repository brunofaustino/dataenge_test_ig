import os
import logging
from pathlib import Path
import pandas as pd
from data_processor import CommonCrawlProcessor
from metrics_processor import MetricsProcessor

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Compute metrics from processed data."""
    try:

        processor = CommonCrawlProcessor()
        df = processor.read_from_arrow(Path("/data/processed"))
        
        metrics_processor = MetricsProcessor()
        metrics = metrics_processor.compute_metrics(df)
        
        db_params = {
            'dbname': os.environ['POSTGRES_DB'],
            'user': os.environ['POSTGRES_USER'],
            'password': os.environ['POSTGRES_PASSWORD'],
            'host': os.environ['POSTGRES_HOST'],
            'port': '5432'
        }
        
        processor = CommonCrawlProcessor(db_params=db_params)
        processor.save_metrics_to_postgres(metrics)
        logger.info("Successfully saved metrics to PostgreSQL")
        
    except Exception as e:
        logger.error(f"Error computing metrics: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 