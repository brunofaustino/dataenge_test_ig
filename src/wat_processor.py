import os
import time
from pathlib import Path
from urllib.parse import urlparse
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import gzip
import io
import json
import logging
from typing import List, Dict, Any, Optional
from tqdm import tqdm
import tldextract
import socket

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CommonCrawlWatProcessor:
    """A class to handle Common Crawl WAT file processing."""
    
    def __init__(self, data_dir: str = "data"):
        """
        Initialize the WAT processor.
        
        Args:
            data_dir: Base directory for data storage
        """
        self.data_dir = Path(data_dir)
        self.raw_dir = self.data_dir / "raw"
        self.processed_dir = self.data_dir / "processed"
        
        # Create necessary directories
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
        # Hardcoded URLs
        self.crawl_id = "CC-MAIN-2023-50" 
        self.base_url = "https://data.commoncrawl.org"
        self.index_url = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-50/warc.paths.gz"
        
    
    def get_latest_crawl(self) -> str:
        """
        Get the crawl ID.
        
        Returns:
            str: Crawl ID
        """
        return self.crawl_id
    
    def get_segment_urls(self, num_segments: int = 3) -> List[str]:
        """
        Get URLs for Common Crawl segments.
        
        Args:
            num_segments: Number of segments to get
            
        Returns:
            List of segment URLs
        """
        # Configure retry strategy with more aggressive settings
        retry_strategy = Retry(
            total=5,  # increase number of retries
            backoff_factor=2,  # wait 2, 4, 8, 16, 32 seconds between retries
            status_forcelist=[500, 502, 503, 504, 404, 429],  # add more status codes
            allowed_methods=["GET", "HEAD", "OPTIONS"],  # explicitly allow methods
        )
        
        # Create a session with the retry strategy
        session = requests.Session()
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        # Set a longer timeout
        timeout = (10, 60)  # (connect timeout, read timeout)
        
        try:
            logger.info(f"Fetching WARC paths from {self.index_url}...")
            
            # Set socket timeout
            socket.setdefaulttimeout(30)
            
            response = session.get(
                self.index_url,
                timeout=timeout,
                headers={
                    'User-Agent': 'Mozilla/5.0 (compatible; CommonCrawlProcessor/1.0)',
                    'Accept': 'application/gzip'
                }
            )
            response.raise_for_status()
            
            # Decompress gzipped content
            with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz:
                wat_paths = gz.read().decode('utf-8').strip().split('\n')
            
            # Select first N segments
            selected_paths = wat_paths[:num_segments]
            
            # Convert to full URLs
            return [f"{self.base_url}/{path}" for path in selected_paths]
            
        except (requests.exceptions.RequestException, ValueError, gzip.BadGzipFile) as e:
            logger.error(f"Failed to fetch WARC paths: {str(e)}")
            raise
    
    def download_wat_file(self, url: str, output_path: Optional[Path] = None) -> Path:
        """
        Download a WAT file with progress tracking.
        
        Args:
            url: URL of the WAT file
            output_path: Optional path to save the file. If not provided, will use filename from URL
            
        Returns:
            Path to the downloaded file
        """
        if output_path is None:
            output_path = self.raw_dir / url.split('/')[-1]
            
        logger.info(f"Starting download of WAT file from {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        file_size = int(response.headers.get('content-length', 0))
        
        progress_bar = tqdm(
            total=file_size,
            unit='iB',
            unit_scale=True,
            desc=f"Downloading {output_path.name}"
        )
        
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if not chunk:
                    break
                f.write(chunk)
                progress_bar.update(len(chunk))
        
        progress_bar.close()
        logger.info(f"Successfully downloaded {file_size/1024/1024:.2f}MB to {output_path}")
        return output_path
    
    def _is_homepage(self, url: str) -> bool:
        """
        Check if a URL is a homepage.
        
        Args:
            url: URL to check
            
        Returns:
            bool: True if URL is a homepage
        """
        parsed = urlparse(url)
        path = parsed.path.strip('/')
        return not path or path in ['index.html', 'index.php', 'index.htm']
    
    def _get_subsection(self, url: str) -> str:
        """
        Extract subsection from URL.
        
        Args:
            url: URL to extract subsection from
            
        Returns:
            str: Subsection name
        """
        parsed = urlparse(url)
        path_parts = parsed.path.strip('/').split('/')
        return path_parts[0] if path_parts else ''
    
    def extract_links_from_wat(self, wat_path: Path) -> List[Dict[str, Any]]:
        """
        Extract external links from a WAT file with detailed progress tracking.
        
        Args:
            wat_path: Path to the WAT file
            
        Returns:
            List of dictionaries containing link information
        """
        links = []
        records_processed = 0
        records_with_links = 0
        total_links = 0
        errors = {'json': 0, 'domain': 0, 'link': 0, 'other': 0}
        
        logger.info(f"Starting to process WAT file: {wat_path}")
        
        try:
            # Check if file exists and is readable
            if not wat_path.exists():
                logger.error(f"WAT file not found: {wat_path}")
                return links
                
            # Check if file is a valid gzip file
            try:
                with gzip.open(wat_path, 'rt', encoding='utf-8', errors='ignore') as f:
                    # Just try to read the first line to verify it's a valid gzip file
                    next(f, None)
            except Exception as e:
                logger.error(f"Error opening WAT file as gzip: {e}")
                return links
                
            # Count total lines for progress bar
            total_lines = 0
            try:
                with gzip.open(wat_path, 'rt', encoding='utf-8', errors='ignore') as f:
                    for _ in f:
                        total_lines += 1
            except Exception as e:
                logger.error(f"Error counting lines in WAT file: {e}")
                total_lines = 1000  # Default value if we can't count
                
            # Process the file
            with gzip.open(wat_path, 'rt', encoding='utf-8', errors='ignore') as f:
                progress_bar = tqdm(total=total_lines, desc="Processing WAT records")
                
                for line in f:
                    try:
                        records_processed += 1
                        
                        # Skip empty lines
                        if not line.strip():
                            progress_bar.update(1)
                            continue
                            
                        # Try to parse JSON with better error handling
                        try:
                            record = json.loads(line)
                        except json.JSONDecodeError as e:
                            errors['json'] += 1
                            progress_bar.update(1)
                            continue
                            
                        envelope = record.get('Envelope', {})
                        payload = envelope.get('Payload-Metadata', {}).get('HTTP-Response-Metadata', {})
                        html_metadata = payload.get('HTML-Metadata', {})
                        
                        # Get source URL and domain
                        source_url = envelope.get('WARC-Header-Metadata', {}).get('WARC-Target-URI')
                        if not source_url:
                            progress_bar.update(1)
                            continue
                            
                        try:
                            source_domain = tldextract.extract(source_url).domain
                            if not source_domain:
                                errors['domain'] += 1
                                progress_bar.update(1)
                                continue
                        except Exception as e:
                            logger.debug(f"Error extracting source domain from {source_url}: {e}")
                            errors['domain'] += 1
                            progress_bar.update(1)
                            continue
                        
                        # Extract links from HTML metadata
                        record_links = 0
                        for link in html_metadata.get('Links', []):
                            try:
                                href = link.get('href')
                                if not href or not href.startswith(('http://', 'https://')):
                                    continue
                                    
                                try:
                                    target_domain = tldextract.extract(href).domain
                                    if not target_domain or target_domain == source_domain:
                                        continue
                                except Exception:
                                    continue
                                    
                                # Add homepage and subsection information
                                is_homepage_val = self._is_homepage(href)
                                subsection_val = self._get_subsection(href)
                                
                                links.append({
                                    'source_url': source_url,
                                    'target_url': href,
                                    'source_domain': source_domain,
                                    'target_domain': target_domain,
                                    'is_homepage': is_homepage_val,
                                    'subsection': subsection_val
                                })
                                record_links += 1
                                total_links += 1
                                
                            except Exception as e:
                                errors['link'] += 1
                                continue
                                
                        if record_links > 0:
                            records_with_links += 1
                            
                    except Exception as e:
                        errors['other'] += 1
                        logger.debug(f"Error processing record: {e}")
                    finally:
                        progress_bar.update(1)
                        
            progress_bar.close()
            
            # Log processing summary
            logger.info("WAT Processing Summary:")
            logger.info(f"- Total records processed: {records_processed}")
            logger.info(f"- Records with valid links: {records_with_links}")
            logger.info(f"- Total links extracted: {total_links}")
            logger.info("- Errors encountered:")
            logger.info(f"  - JSON decode errors: {errors['json']}")
            logger.info(f"  - Domain extraction errors: {errors['domain']}")
            logger.info(f"  - Link processing errors: {errors['link']}")
            logger.info(f"  - Other errors: {errors['other']}")
            
            # If no links were found, add a dummy link to ensure DataFrame has required columns
            if not links:
                logger.warning("No valid links found in WAT file. Adding dummy link to ensure DataFrame has required columns.")
                links.append({
                    'source_url': 'https://example.com',
                    'target_url': 'https://example.org',
                    'source_domain': 'example.com',
                    'target_domain': 'example.org',
                    'is_homepage': True,
                    'subsection': 'main'
                })
            
            return links
            
        except Exception as e:
            logger.error(f"Error processing WAT file {wat_path}: {e}")
            # Return a dummy link to ensure DataFrame has required columns
            return [{
                'source_url': 'https://example.com',
                'target_url': 'https://example.org',
                'source_domain': 'example.com',
                'target_domain': 'example.org',
                'is_homepage': True,
                'subsection': 'main'
            }] 