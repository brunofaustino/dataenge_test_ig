import os
import sys
import json
import logging
from pathlib import Path
import pandas as pd
from wat_processor import CommonCrawlWatProcessor
from data_processor import CommonCrawlProcessor
from website_categorizer import WebsiteCategorizer

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_wat_files(segment_urls):
    """Process WAT files and extract links."""
    processor = CommonCrawlWatProcessor()
    all_links = []
    
    for url in segment_urls:
        try:
            wat_path = processor.download_wat_file(url)
            links = processor.extract_links_from_wat(wat_path)
            all_links.extend(links)
            logger.info(f"Found {len(links)} links in {url}")
        except Exception as e:
            logger.error(f"Error processing {url}: {e}")
            continue
    
    return all_links

def categorize_websites(links):
    """Categorize websites from extracted links."""
    df = pd.DataFrame(links)
    
    processor = CommonCrawlProcessor()
    df['is_homepage'] = df['target_url'].apply(processor._is_homepage)
    df['subsection'] = df['target_url'].apply(processor._get_subsection)
    
    unique_domains = df['target_domain'].unique()
    logger.info(f"Found {len(unique_domains)} unique domains to categorize")
    
    categorizer = WebsiteCategorizer()
    
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

def save_to_postgres(df, metrics):
    """Save processed data to PostgreSQL."""
    db_params = {
        'dbname': os.environ['POSTGRES_DB'],
        'user': os.environ['POSTGRES_USER'],
        'password': os.environ['POSTGRES_PASSWORD'],
        'host': os.environ['POSTGRES_HOST'],
        'port': '5432'
    }
    
    processor = CommonCrawlProcessor(db_params=db_params)
    processor.load_to_postgres(df, metrics)
    logger.info("Successfully saved data to PostgreSQL")

def save_to_arrow(df):
    """Save processed data to Arrow/Parquet format."""
    processor = CommonCrawlProcessor()
    processor.save_to_arrow(df, partition_cols=['target_domain'], output_dir=Path("/data/processed"))
    logger.info("Successfully saved data to Arrow/Parquet")

def main():
    """Main processing function."""
    try:
        segment_urls = json.loads(sys.argv[1])
        
        links = process_wat_files(segment_urls)
        logger.info(f"Found {len(links)} links in total")
        
        df = categorize_websites(links)
        
        processor = CommonCrawlProcessor()
        metrics = processor.compute_metrics(df)
        
        save_to_postgres(df, metrics)
        save_to_arrow(df)
        
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 