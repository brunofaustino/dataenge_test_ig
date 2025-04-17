import logging
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import List, Dict, Any

from src.wat_processor import CommonCrawlWatProcessor
from src.data_processor import CommonCrawlProcessor
from src.website_categorizer import WebsiteCategorizer
from src.metrics_processor import MetricsProcessor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def process_wat_files(num_segments: int = 1) -> List[Dict[str, Any]]:
    """
    Process WAT files from Common Crawl.
    
    Args:
        num_segments: Number of segments to process
        
    Returns:
        List of extracted links
    """
    # Initialize processor
    processor = CommonCrawlWatProcessor()
    
    # Get segment URLs
    segment_urls = processor.get_segment_urls(num_segments)
    
    # Process each segment
    all_links = []
    for url in segment_urls:
        # Download WAT file
        wat_path = processor.download_wat_file(url)
        
        # Extract links
        links = processor.extract_links_from_wat(wat_path)
        all_links.extend(links)
        
        logger.info(f"Found {len(links)} links in {url}")
    
    logger.info(f"\nFound {len(all_links)} links in total")
    return all_links

def categorize_websites(links: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Categorize websites from extracted links.
    
    Args:
        links: List of extracted links
        
    Returns:
        DataFrame with categorized websites
    """
    # Convert to DataFrame
    df = pd.DataFrame(links)
    
    # Add homepage and subsection flags
    processor = CommonCrawlProcessor()
    df['is_homepage'] = df['target_url'].apply(processor._is_homepage)
    df['subsection'] = df['target_url'].apply(processor._get_subsection)
    
    # Get unique domains
    unique_domains = df['target_domain'].unique()
    logger.info(f"\nCategorizing websites...")
    logger.info(f"Found {len(unique_domains)} unique domains to categorize")
    
    # Initialize categorizer
    categorizer = WebsiteCategorizer()
    
    # Process each domain in batches
    batch_size = 100
    for i in range(0, len(unique_domains), batch_size):
        batch = unique_domains[i:i + batch_size]
        for domain in batch:
            try:
                # Get a sample URL for this domain
                sample_url = df[df['target_domain'] == domain]['target_url'].iloc[0]
                result = categorizer.categorize_website(sample_url)
                
                # Update DataFrame with categorization results
                mask = df['target_domain'] == domain
                df.loc[mask, 'country'] = result['country']
                df.loc[mask, 'category'] = result['category']
                df.loc[mask, 'is_ad_based'] = result['is_ad_based']
            except Exception as e:
                logger.error(f"Error categorizing {domain}: {e}")
                continue
    
    return df

def save_data(df: pd.DataFrame, metrics: Dict[str, Any], db_params: Dict[str, str]):
    """
    Save processed data to various formats.
    
    Args:
        df: DataFrame with processed data
        metrics: Dictionary of computed metrics
        db_params: Database connection parameters
    """
    # Save to PostgreSQL
    processor = CommonCrawlProcessor(db_params=db_params)
    processor.load_to_postgres(df, metrics)
    
    # Save to Arrow/Parquet
    logger.info("\nSaving to Arrow/Parquet...")
    table = pa.Table.from_pandas(df)
    pq.write_table(table, "data/processed/links.parquet")
    
    # Save metrics using MetricsProcessor
    metrics_processor = MetricsProcessor()
    metrics_processor.save_metrics(metrics)
    metrics_processor.generate_report(metrics)

def main():
    """Main function to run the processing pipeline."""
    # Process WAT files (just one segment for testing)
    links = process_wat_files(num_segments=1)
    
    # Categorize websites
    df = categorize_websites(links)
    
    # Compute metrics using MetricsProcessor
    metrics_processor = MetricsProcessor()
    metrics = metrics_processor.compute_metrics(df)
    
    # Save data
    db_params = {
        'dbname': 'crawldata',
        'user': 'dataengineer',
        'password': 'dataengineer',
        'host': 'localhost',
        'port': '5432'
    }
    save_data(df, metrics, db_params)
    
    logger.info("\nProcessing complete!")

if __name__ == "__main__":
    main() 