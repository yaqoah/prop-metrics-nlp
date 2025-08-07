import json
import tempfile
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from scrapy.crawler import CrawlerProcess
import os
from .base import Base
from parsers.trustpilot import Trustpilot
from utils.constants import *
from .scrapy_spider import Trustpilot_Spider
from api.data_models import Review

class Scrapy_Manager(Base):
    def __init__(self):
        super().__init__("Scrapy")
        self.parser = Trustpilot()

    def _create_process(self):
        settings = {
            'USER_AGENT': self.get_random_user_agent(),
            'ROBOTSTXT_OBEY': False,
            'CONCURRENT_REQUESTS': 4,
            'DOWNLOAD_DELAY': 2,
            'COOKIES_ENABLED': True,
            'RETRY_TIMES': 3,
            'RETRY_HTTP_CODES': [500, 502, 503, 504, 408, 429, 403],
            'LOG_LEVEL': 'WARNING',
            
            # Error handling
            'SPIDER_MIDDLEWARES': {
                'scrapy.spidermiddlewares.httperror.HttpErrorMiddleware': 50,
            },
            
            # Proxy handling
            'DOWNLOADER_MIDDLEWARES': {
                'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 110,
                'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
            }
        }
        
        return CrawlerProcess(settings)
    
    def _run_spider(self, url, start_page):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            output_file = f.name
        
        try:
            # Create and configure process
            process = self._create_process()

            # Modify URL to include the starting page if not page 1
            if start_page > 1:
                if '?' in url:
                    spider_url = f"{url}&page={start_page}"
                else:
                    spider_url = f"{url}?page={start_page}"
            else:
                spider_url = url
            
            # Run spider with proxy rotator
            process.crawl(
                Trustpilot_Spider, 
                start_url=spider_url, 
                output_file=output_file,
                proxy_rotator=self.proxy_rotator,
                start_page=start_page
            )
            process.start()
            
            # Read results
            with open(output_file, 'r') as f:
                return json.load(f)
                
        finally:
            # Cleanup
            if os.path.exists(output_file):
                os.unlink(output_file)
    
    def scrapes_info(self, url):
        try:
            for attempt in range(MAX_RETRIES):
                proxy = self.get_proxy()
                try:
                    headers = {'User-Agent': self.get_random_user_agent()}
                    response = requests.get(
                        url,
                        headers=headers,
                        proxies=proxy if proxy else None,
                        timeout=(3,5)
                    )
                    
                    if response.status_code == 200:
                        soup = BeautifulSoup(response.text, 'html.parser')
                        return self.parser.parse_firm(soup, url)
                    elif response.status_code == 403 and proxy:
                        self.mark_proxy_failed(proxy)
                        
                except Exception as e:
                    self.logger.error(f"Attempt {attempt + 1} failed: {e}")
                    if proxy:
                        self.mark_proxy_failed(proxy)
            
            # If simple requests fail, fall back to full Scrapy spider
            self.logger.info("Simple request failed, using Scrapy spider")
            data = self._run_spider(url)
            return data.get('firm_info')
            
        except Exception as e:
            self.logger.error(f"Error scraping firm info: {e}")
            return None
    
    def scrapes_reviews_page(self, url, page):
        try:
            data = self._run_spider(url, start_page=page)
            
            # Convert raw review data to Review objects
            reviews = []
            for review_data in data.get('reviews', []):
                try:
                    if not isinstance(review_data, dict):
                        self.logger.error(f"Expected dict, got {type(review_data)}: {review_data}")
                        continue
                    
                    # Handle datetime conversion
                    for date_field in ['date_posted', 'date_of_experience', 'reply_date']:
                        if date_field in review_data and isinstance(review_data[date_field], str):
                            review_data[date_field] = datetime.fromisoformat(review_data[date_field])

                    review = Review(**review_data)
                    reviews.append(review)
                except Exception as e:
                    self.logger.error(f"Error creating Review object: {e}")
            
            # Store results for subsequent page requests
            self._cached_results = {
                'reviews': reviews,
                'has_next': data.get('has_next', False),
                'total_pages': len(reviews) // 20 + 1  # Estimate
            }
            
            # Return all reviews at once since Scrapy processes all pages
            return reviews, False  # No need for pagination with Scrapy
                
        except Exception as e:
            self.logger.error(f"Error scraping reviews: {e}")
            return [], False