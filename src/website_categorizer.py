import logging
from typing import Dict, Optional
import socket
import requests
import whois
from tldextract import extract
from ipwhois import IPWhois
import json
from pathlib import Path

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
        self.content_categories = self._load_content_categories()

    def _load_ad_domains(self) -> set:
        """Load list of known ad-based domains."""
        ad_domains_file = Path("data/ad_domains.json")
        if ad_domains_file.exists():
            with open(ad_domains_file) as f:
                return set(json.load(f))
        
        # Default list of known ad domains
        return {
            "doubleclick.net",
            "googleadservices.com",
            "adnxs.com",
            "advertising.com",
            "adsrvr.org",
            "adroll.com",
            "adform.net",
            "advertising.com",
            "adtechus.com",
            "advertising.com",
            "adcolony.com",
            "adform.net",
            "adroll.com",
            "adsrvr.org",
            "adtechus.com",
            "advertising.com",
            "adcolony.com",
            "adform.net",
            "adroll.com",
            "adsrvr.org",
            "adtechus.com",
            "advertising.com",
            "adcolony.com"
        }

    def _load_content_categories(self) -> Dict[str, str]:
        """Load content category mappings."""
        categories_file = Path("data/content_categories.json")
        if categories_file.exists():
            with open(categories_file) as f:
                return json.load(f)
        return {}

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

    def _get_content_category(self, domain: str) -> Optional[str]:
        """
        Get website content category using Clearbit Logo API.
        
        Args:
            domain: Website domain
            
        Returns:
            Content category or None if categorization failed
        """
        if not self.api_key:
            return None
            
        try:
            # Use Clearbit Logo API to get company info
            response = requests.get(
                f"https://logo.clearbit.com/{domain}",
                headers={"Authorization": f"Bearer {self.api_key}"}
            )
            
            if response.status_code == 200:
                # Get company info from Clearbit
                company_response = requests.get(
                    f"https://company.clearbit.com/v2/companies/find?domain={domain}",
                    headers={"Authorization": f"Bearer {self.api_key}"}
                )
                
                if company_response.status_code == 200:
                    company_data = company_response.json()
                    category = company_data.get('category', {}).get('name')
                    if category:
                        return category
            
            # Fallback to manual categorization
            return self._categorize_manually(domain)
            
        except Exception as e:
            logger.error(f"Error getting content category for {domain}: {e}")
            return self._categorize_manually(domain)

    def _categorize_manually(self, domain: str) -> Optional[str]:
        """
        Manually categorize website based on domain patterns.
        
        Args:
            domain: Website domain
            
        Returns:
            Content category or None if categorization failed
        """
        # Check against known categories
        for pattern, category in self.content_categories.items():
            if pattern in domain:
                return category
        
        # Default categories based on common TLDs and patterns
        if domain.endswith('.edu'):
            return 'Education'
        elif domain.endswith('.gov'):
            return 'Government'
        elif domain.endswith('.org'):
            return 'Non-Profit'
        elif 'shop' in domain or 'store' in domain:
            return 'E-Commerce'
        elif 'blog' in domain:
            return 'Blog'
        elif 'news' in domain:
            return 'News'
        
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
            
            # Get content category
            category = self._get_content_category(domain)
            
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