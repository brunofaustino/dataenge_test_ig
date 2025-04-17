import logging
from typing import Dict, Optional
import socket
import requests
import whois
from tldextract import extract
from ipwhois import IPWhois

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WebsiteCategorizer:
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the website categorizer.
        
        Args:
            api_key: API key for content categorization service
        """
        self.api_key = api_key
        self.ad_domains = self._load_ad_domains()

    def _load_ad_domains(self) -> set:
        """Load list of known ad-based domains."""
        # This would typically load from a file or database
        # For now, returning a small sample set
        return {
            "doubleclick.net",
            "googleadservices.com",
            "adnxs.com",
            "advertising.com"
        }

    def _get_country_from_ip(self, domain: str) -> Optional[str]:
        """
        Get country from IP address using ipwhois.
        
        Args:
            domain: Domain to get country for
            
        Returns:
            Country code or None if not found
        """
        try:
            # Get IP address for domain
            ip = socket.gethostbyname(domain)
            logger.info(f"IP for {domain}: {ip}")
            
            # Get country using ipwhois
            whois = IPWhois(ip)
            result = whois.lookup_rdap()
            country_code = result['network']['country']
            logger.info(f"Country code for {domain}: {country_code}")
            
            return country_code if country_code else None
            
        except Exception as e:
            logger.error(f"Error getting country for {domain}: {e}")
            return None

    def categorize_website(self, url: str) -> Dict:
        """
        Categorize a website by content type and country.
        
        Args:
            url: Website URL to categorize
            
        Returns:
            Dictionary containing categorization information
        """
        try:
            domain = extract(url).domain
            
            # Get country from IP
            country = self._get_country_from_ip(domain)
            logger.info(f"Country for {domain}: {country}")
            
            # Get content category using external API
            category = self._get_content_category(url)
            
            # Check if domain is ad-based
            is_ad_based = domain in self.ad_domains
            
            return {
                "domain": domain,
                "country": country,
                "category": category,
                "is_ad_based": is_ad_based
            }
            
        except Exception as e:
            logger.error(f"Error categorizing website {url}: {e}")
            return {
                "domain": extract(url).domain,
                "country": None,
                "category": None,
                "is_ad_based": False
            }

    def _get_content_category(self, url: str) -> Optional[str]:
        """
        Get website content category using external API.
        
        Args:
            url: Website URL
            
        Returns:
            Content category or None if categorization failed
        """
        if not self.api_key:
            return None
            
        try:
            # This is a placeholder for the actual API call
            # You would implement the actual API call here
            response = requests.get(
                "https://api.example.com/categorize",
                params={"url": url, "api_key": self.api_key}
            )
            response.raise_for_status()
            return response.json()["category"]
            
        except Exception as e:
            logger.error(f"Error getting content category for {url}: {e}")
            return None 