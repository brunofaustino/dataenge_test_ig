import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
import json

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import tldextract
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from urllib.parse import urlparse
from warcio.archiveiterator import ArchiveIterator
import psycopg2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CommonCrawlProcessor:
    """Process Common Crawl data to extract external links and metadata."""
    
    def __init__(self, db_params: Dict[str, str] = None, data_dir: Path = None):
        self.base_url = "https://data.commoncrawl.org"
        self.index_url = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-50/warc.paths.gz"
        self.data_dir = data_dir or Path("data")
        self.raw_dir = self.data_dir / "raw"
        self.processed_dir = self.data_dir / "processed"
        self.final_dir = self.data_dir / "final"
        self.db_params = db_params or {
            'dbname': 'crawldata',
            'user': 'dataengineer',
            'password': 'dataengineer',
            'host': 'localhost',
            'port': '5432'
        }
        self.ensure_directories()

    def ensure_directories(self):
        """Create necessary directories if they don't exist."""
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        self.final_dir.mkdir(parents=True, exist_ok=True)

    def download_segment(self, segment_url: str) -> Path:
        """
        Download a Common Crawl segment.
        
        Args:
            segment_url (str): URL of the segment to download
            
        Returns:
            Path: Path to the downloaded file
        """
        output_path = self.data_dir / "raw" / Path(segment_url).name
        
        if not output_path.exists():
            logger.info(f"Downloading {segment_url}...")
            response = requests.get(segment_url, stream=True)
            response.raise_for_status()
            
            with open(output_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"Downloaded to {output_path}")
        else:
            logger.info(f"Segment already exists: {output_path}")
        
        return output_path

    def extract_links(self, warc_file: Path, max_records=None) -> List[Dict]:
        """
        Extract external links from a WARC file.
        
        Args:
            warc_file (Path): Path to the WARC file
            max_records (int, optional): Maximum number of records to process
            
        Returns:
            list: List of dictionaries containing link information
        """
        links = []
        record_count = 0
        
        with open(warc_file, 'rb') as stream:
            for record in ArchiveIterator(stream):
                if record.rec_type == 'response':
                    try:
                        if record.http_headers.get_header('Content-Type', '').startswith('text/html'):
                            html = record.content_stream().read().decode('utf-8', errors='ignore')
                            source_url = record.rec_headers.get_header('WARC-Target-URI', '')
                            
                            # Extract links from HTML
                            soup = BeautifulSoup(html, "html.parser")
                            source_domain = tldextract.extract(source_url).domain
                            
                            for link in soup.find_all("a", href=True):
                                href = link["href"]
                                if href.startswith(("http://", "https://")):
                                    target_domain = tldextract.extract(href).domain
                                    if target_domain and target_domain != source_domain:
                                        links.append({
                                            "source_url": source_url,
                                            "source_domain": source_domain,
                                            "target_url": href,
                                            "target_domain": target_domain
                                        })
                            
                            record_count += 1
                            if max_records and record_count >= max_records:
                                break
                    except Exception as e:
                        logger.error(f"Error processing record: {e}")
        
        return links

    def _is_homepage(self, url: str) -> bool:
        """Check if a URL is a homepage."""
        path = urlparse(url).path
        return path in ("", "/", "/index.html", "/index.htm")

    def _get_subsection(self, url: str) -> Optional[str]:
        """Extract subsection from URL."""
        path = urlparse(url).path
        parts = [p for p in path.split("/") if p]
        return parts[0] if parts else None

    def load_to_postgres(self, df: pd.DataFrame, metrics: Dict[str, Any]) -> None:
        """
        Load processed data into PostgreSQL.
        
        Args:
            df: DataFrame with processed data
            metrics: Dictionary of computed metrics
        """
        logger.info(f"\nLoading {len(df)} records to {metrics}")
        
        # NOTE: In a real environment, we don't need to create tables, we will use existing tables not managed by Airflow
        table_schemas = {
            'external_links': """
                CREATE TABLE IF NOT EXISTS external_links (
                    id SERIAL PRIMARY KEY,
                    source_url TEXT,
                    target_url TEXT,
                    target_domain TEXT,
                    is_homepage BOOLEAN,
                    subsection TEXT,
                    country TEXT,
                    category TEXT,
                    is_ad_based BOOLEAN,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            'website_metrics': """
                CREATE TABLE IF NOT EXISTS website_metrics (
                    id SERIAL PRIMARY KEY,
                    metric_name TEXT,
                    metric_value JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
        }
        
        conn = None
        cur = None
        
        try:
            conn = psycopg2.connect(**self.db_params)
            cur = conn.cursor()
            
            for table_name, schema in table_schemas.items():
                cur.execute(schema)
            
            for _, row in df.iterrows():
                cur.execute("""
                    INSERT INTO external_links 
                    (source_url, target_url, target_domain, is_homepage, subsection, country, category, is_ad_based)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['source_url'],
                    row['target_url'],
                    row['target_domain'],
                    row['is_homepage'],
                    row['subsection'],
                    row.get('country'),
                    row.get('category'),
                    row.get('is_ad_based', False)
                ))
            
            cur.execute("""
                INSERT INTO website_metrics (metric_name, metric_value)
                VALUES (%s, %s)
            """, ('crawl_metrics', json.dumps(metrics)))
            
            conn.commit()
            logger.info("Successfully loaded data to PostgreSQL")
            
        except Exception as e:
            logger.error(f"Error loading data to PostgreSQL: {str(e)}")
            if conn:
                conn.rollback()
            raise
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    def save_to_arrow(self, df: pd.DataFrame, partition_cols: List[str], output_dir: Path):
        """
        Save DataFrame to Arrow/Parquet format with partitioning.
        
        Args:
            df: DataFrame to save
            partition_cols: Columns to partition by
            output_dir: Output directory
        """
        table = pa.Table.from_pandas(df)
        pq.write_to_dataset(
            table,
            root_path=str(output_dir),
            partition_cols=partition_cols
        )

    def compute_metrics(self, df: pd.DataFrame) -> Dict:
        """
        Compute metrics from the processed data.
        
        Args:
            df: DataFrame with processed data
            
        Returns:
            Dictionary containing computed metrics
        """
        # Check if DataFrame is empty or missing required columns
        required_columns = ["target_domain", "is_homepage", "subsection"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.warning(f"DataFrame is missing required columns: {missing_columns}. Using default values.")
            # Add missing columns with default values
            for col in missing_columns:
                if col == "target_domain":
                    df[col] = "example.org"
                elif col == "is_homepage":
                    df[col] = True
                elif col == "subsection":
                    df[col] = "main"
        
        # Check if DataFrame is empty
        if df.empty:
            logger.warning("DataFrame is empty. Using default values for metrics.")
            return {
                "total_unique_domains": 0,
                "homepage_ratio": 0.0,
                "avg_subsections_per_domain": 0.0,
                "top_categories": {},
                "geographic_distribution": {}
            }
        
        # Compute metrics with error handling
        try:
            metrics = {
                "total_unique_domains": df["target_domain"].nunique(),
                "homepage_ratio": (df["is_homepage"].sum() / len(df)) * 100 if len(df) > 0 else 0.0,
                "avg_subsections_per_domain": df.groupby("target_domain")["subsection"].nunique().mean() if len(df) > 0 else 0.0,
            }
            
            # Add optional metrics if columns exist
            if "category" in df.columns:
                metrics["top_categories"] = df["category"].value_counts().head(10).to_dict()
            else:
                metrics["top_categories"] = {}
                
            if "country" in df.columns:
                metrics["geographic_distribution"] = df["country"].value_counts().head(10).to_dict()
            else:
                metrics["geographic_distribution"] = {}
                
            return metrics
            
        except Exception as e:
            logger.error(f"Error computing metrics: {e}")
            # Return default metrics in case of error
            return {
                "total_unique_domains": 0,
                "homepage_ratio": 0.0,
                "avg_subsections_per_domain": 0.0,
                "top_categories": {},
                "geographic_distribution": {}
            }

    def get_latest_crawl(self):
        """
        Get the latest Common Crawl index.
        
        Returns:
            str: Latest crawl index
        """
        response = requests.get("https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-10/warc.paths.gz")
        warc_paths = response.content.decode('utf-8').strip().split('\n')
        return warc_paths[0].split('/')[2]  # Extract crawl index from first path

    def process_segments(self, segment_urls: List[str], max_records_per_segment=None) -> pd.DataFrame:
        """
        Process multiple segments.
        
        Args:
            segment_urls (list): List of segment URLs to process
            max_records_per_segment (int, optional): Maximum records to process per segment
            
        Returns:
            DataFrame: DataFrame containing all extracted links
        """
        all_links = []
        
        for segment_url in segment_urls:
            logger.info(f"\nProcessing segment: {segment_url}")
            warc_file = self.data_dir / "raw" / Path(segment_url).name
            
            if not warc_file.exists():
                warc_file = self.download_segment(segment_url)
            
            links = self.extract_links(warc_file, max_records=max_records_per_segment)
            all_links.extend(links)
        
        return pd.DataFrame(all_links)

def download_warc_file(url: str, output_path: str, max_size_mb: int = 100) -> str:
    """
    Download a portion of a WARC file from the given URL.
    
    Args:
        url: URL of the WARC file
        output_path: Path to save the downloaded file
        max_size_mb: Maximum size to download in megabytes (default: 100MB)
        
    Returns:
        Path to the downloaded file
    """
    logger.info(f"Downloading first {max_size_mb}MB of WARC file from {url}")
    
    # Convert MB to bytes
    max_size = max_size_mb * 1024 * 1024
    
    headers = {'Range': f'bytes=0-{max_size-1}'}
    response = requests.get(url, headers=headers, stream=True)
    response.raise_for_status()
    
    with open(output_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    logger.info(f"Downloaded {max_size_mb}MB to {output_path}")
    return output_path

def extract_links_from_warc(warc_path: str) -> List[Dict[str, Any]]:
    """
    Extract external links from a WARC file.
    
    Args:
        warc_path: Path to the WARC file
        
    Returns:
        List of dictionaries containing link information
    """
    links = []
    
    with open(warc_path, 'rb') as f:
        for record in ArchiveIterator(f):
            if record.rec_type == 'response':
                try:
                    # Check content type before processing
                    content_type = record.http_headers.get_header('Content-Type', '')
                    if not content_type.startswith('text/html'):
                        continue
                        
                    content = record.content_stream().read().decode('utf-8', errors='ignore')
                    source_url = record.rec_headers.get_header('WARC-Target-URI')
                    
                    # Parse HTML content with BeautifulSoup
                    soup = BeautifulSoup(content, 'html.parser', from_encoding='utf-8')
                    
                    # Extract source domain
                    try:
                        source_domain = tldextract.extract(source_url).domain
                    except Exception as e:
                        logger.error(f"Error extracting source domain: {e}")
                        continue
                    
                    # Find all links
                    for link in soup.find_all('a', href=True):
                        try:
                            href = link['href']
                            
                            # Skip non-HTTP URLs and fragments
                            if not href.startswith(('http://', 'https://')):
                                continue
                                
                            # Extract target domain
                            target_domain = tldextract.extract(href).domain
                            
                            # Skip if no valid domain or same domain
                            if not target_domain or target_domain == source_domain:
                                continue
                                
                            links.append({
                                'source_url': source_url,
                                'target_url': href,
                                'source_domain': source_domain,
                                'target_domain': target_domain
                            })
                        except Exception as e:
                            logger.error(f"Error processing link: {e}")
                            continue
                            
                except Exception as e:
                    logger.error(f"Error processing record: {e}")
                    continue
    
    return links

def save_to_postgres(links: List[Dict[str, Any]], db_params: Dict[str, str]) -> None:
    """
    Save extracted links to PostgreSQL.
    
    Args:
        links: List of link dictionaries
        db_params: Database connection parameters
    """
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS external_links (
            id SERIAL PRIMARY KEY,
            source_url TEXT,
            target_url TEXT,
            source_domain TEXT,
            target_domain TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Insert data
    for link in links:
        cur.execute("""
            INSERT INTO external_links (source_url, target_url, source_domain, target_domain)
            VALUES (%s, %s, %s, %s)
        """, (
            link['source_url'],
            link['target_url'],
            link['source_domain'],
            link['target_domain']
        ))
    
    conn.commit()
    cur.close()
    conn.close()

def save_to_arrow(links: List[Dict[str, Any]], output_path: str) -> None:
    """
    Save extracted links to Arrow/Parquet format.
    
    Args:
        links: List of link dictionaries
        output_path: Path to save the Arrow/Parquet file
    """
    if not links:
        logger.warning("No links to save")
        return
        
    df = pd.DataFrame(links)
    
    df['domain_bucket'] = df['source_domain'].apply(lambda x: hash(x) % 100)
    
    table = pa.Table.from_pandas(df)
    
    pq.write_to_dataset(
        table,
        root_path=output_path,
        partition_cols=['domain_bucket']
    )

def process_segment(segment_url: str, output_dir: str, db_params: Dict[str, str], max_size_mb: int = 100) -> None:
    """
    Process a single Common Crawl segment.
    
    Args:
        segment_url: URL of the Common Crawl segment
        output_dir: Directory to save processed data
        db_params: Database connection parameters
        max_size_mb: Maximum size to download in megabytes (default: 100MB)
    """
    # Download WARC file
    warc_path = f"{output_dir}/raw/{segment_url.split('/')[-1]}"
    download_warc_file(segment_url, warc_path, max_size_mb)
    
    links = extract_links_from_warc(warc_path)
    
    save_to_postgres(links, db_params)
    
    arrow_path = f"{output_dir}/final/links_{segment_url.split('/')[-1].replace('.warc.gz', '')}"
    save_to_arrow(links, arrow_path)
    
    logger.info(f"Processed segment {segment_url}") 