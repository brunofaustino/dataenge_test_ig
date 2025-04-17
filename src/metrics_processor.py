import logging
import pandas as pd
from typing import Dict, Any, List, Optional
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

logger = logging.getLogger(__name__)

class MetricsProcessor:
    """A class to handle metrics computation and storage for Common Crawl data."""
    
    def __init__(self, data_dir: str = "data"):
        """
        Initialize the metrics processor.
        
        Args:
            data_dir: Base directory for data storage
        """
        self.data_dir = Path(data_dir)
        self.processed_dir = self.data_dir / "processed"
        self.processed_dir.mkdir(parents=True, exist_ok=True)
    
    def compute_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Compute metrics from the processed data.
        
        Args:
            df: DataFrame with processed data
            
        Returns:
            Dictionary of computed metrics
        """
        logger.info("\nComputing metrics...")
        
        metrics = {
            'total_unique_domains': df['target_domain'].nunique(),
            'homepage_ratio': df['is_homepage'].mean(),
            'avg_subsections_per_domain': df.groupby('target_domain')['subsection'].nunique().mean(),
        }
        
        # Top domains by link count
        top_domains = df['target_domain'].value_counts().head(10).to_dict()
        metrics['top_domains'] = top_domains
        
        # Domain categories (if available)
        if 'category' in df.columns and df['category'].notna().any():
            top_categories = df['category'].value_counts().head(10).to_dict()
            metrics['top_categories'] = top_categories
        else:
            metrics['top_categories'] = {}
        
        # Geographic distribution (if available)
        if 'country' in df.columns and df['country'].notna().any():
            geo_dist = df['country'].value_counts().to_dict()
            metrics['geographic_distribution'] = geo_dist
        else:
            metrics['geographic_distribution'] = {}
        
        # Ad-based domains (if available)
        if 'is_ad_based' in df.columns and df['is_ad_based'].notna().any():
            ad_ratio = df['is_ad_based'].mean()
            metrics['ad_based_ratio'] = ad_ratio
        
        logger.info("\nMetrics:")
        for key, value in metrics.items():
            logger.info(f"{key}: {value}")
        
        return metrics
    
    def save_metrics(self, metrics: Dict[str, Any], format: str = "parquet") -> Path:
        """
        Save metrics to file.
        
        Args:
            metrics: Dictionary of metrics to save
            format: Output format ('parquet' or 'csv')
            
        Returns:
            Path to the saved metrics file
        """
        processed_metrics = {}
        for key, value in metrics.items():
            if isinstance(value, dict) and not value:
                processed_metrics[key] = None
            else:
                processed_metrics[key] = value
        
        metrics_df = pd.DataFrame([processed_metrics])
        
        if format.lower() == "parquet":
            output_path = self.processed_dir / "metrics.parquet"
            table = pa.Table.from_pandas(metrics_df)
            pq.write_table(table, output_path)
        else:
            output_path = self.processed_dir / "metrics.csv"
            metrics_df.to_csv(output_path, index=False)
        
        logger.info(f"Saved metrics to {output_path}")
        return output_path
    
    def load_metrics(self, format: str = "parquet") -> pd.DataFrame:
        """
        Load metrics from file.
        
        Args:
            format: Input format ('parquet' or 'csv')
            
        Returns:
            DataFrame containing metrics
        """
        if format.lower() == "parquet":
            input_path = self.processed_dir / "metrics.parquet"
            table = pq.read_table(input_path)
            return table.to_pandas()
        else:
            input_path = self.processed_dir / "metrics.csv"
            return pd.read_csv(input_path)
    
    def generate_report(self, metrics: Dict[str, Any], output_path: Optional[Path] = None) -> Path:
        """
        Generate a human-readable report from metrics.
        
        Args:
            metrics: Dictionary of metrics
            output_path: Optional path for the report file
            
        Returns:
            Path to the generated report
        """
        if output_path is None:
            output_path = self.processed_dir / "metrics_report.txt"
        
        with open(output_path, 'w') as f:
            f.write("Common Crawl Metrics Report\n")
            f.write("=========================\n\n")
            
            f.write("Basic Metrics:\n")
            f.write(f"- Total unique domains: {metrics['total_unique_domains']}\n")
            f.write(f"- Homepage ratio: {metrics['homepage_ratio']:.2%}\n")
            f.write(f"- Average subsections per domain: {metrics['avg_subsections_per_domain']:.2f}\n\n")
            
            f.write("Top Domains:\n")
            for domain, count in metrics.get('top_domains', {}).items():
                f.write(f"- {domain}: {count} links\n")
            f.write("\n")
            
            if metrics.get('top_categories'):
                f.write("Top Categories:\n")
                for category, count in metrics['top_categories'].items():
                    f.write(f"- {category}: {count} domains\n")
                f.write("\n")
            
            if metrics.get('geographic_distribution'):
                f.write("Geographic Distribution:\n")
                for country, count in metrics['geographic_distribution'].items():
                    f.write(f"- {country}: {count} domains\n")
                f.write("\n")
            
            if 'ad_based_ratio' in metrics:
                f.write(f"Ad-based domains ratio: {metrics['ad_based_ratio']:.2%}\n")
        
        logger.info(f"Generated metrics report at {output_path}")
        return output_path 