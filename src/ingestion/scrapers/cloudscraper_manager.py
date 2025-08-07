import cloudscraper
import re
from bs4 import BeautifulSoup
from parsers.trustpilot import Trustpilot
from utils.constants import REQUEST_TIMEOUT
from utils.exceptions import ScrapingError
import time
from .base import Base
from utils.constants import *

class Cloudscraper_Manager(Base):
    def __init__(self):
        super().__init__("CloudScraper")
        self.parser = Trustpilot()

    def _create_scraper(self):
        return cloudscraper.create_scraper(
            browser = {
                "browser": "chrome",
                "platform": "windows",
                "mobile": False
            }
        )
    
    def _make_request(self, url):
        for _ in range(MAX_RETRIES):
            scraper = self._create_scraper()
            try:
                response = scraper.get(url, timeout=REQUEST_TIMEOUT)
                if response.status_code == 200:
                    return response.text
                elif response.status_code == 403:
                    self.logger.warning(f"Error {response.status_code} - Blocked: Fallback to Scrapy")
                    continue
                else:
                    self.logger.warning(f"Got status {response.status_code}")
            except Exception as e:
                self.logger.error(f"Request failed: {e}")

        return None
        
    def scrapes_info(self, url):
        try:
            self.rate_limit()
            html = self._make_request(url)
            if html:
                soup = BeautifulSoup(html, "html.parser")
                return self.parser.parse_firm(soup, url)
        except Exception as e:
            self.logger.error(f"{e} Error scraping firm info")
            return None

    def scrapes_reviews_page(self, url, page):
        try:
            page_url = f"{url}?page={page}" if page > 1 else url
            self.rate_limit()
            html = self._make_request(page_url)
            if html:
                soup = BeautifulSoup(html, 'html.parser')
                reviews = self.parser.parse_reviews(soup)
                has_next = self.parser.has_next_page(soup)
                self.logger.info(f"Scraped {len(reviews)} reviews from page {page}")
                return reviews, has_next
        except Exception as e:
            self.logger.error(f"[{e}] Error scraping reviews page number {page}")
            return [], False
