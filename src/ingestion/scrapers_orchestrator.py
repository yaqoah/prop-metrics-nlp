import json
import time
import random
from collections import Counter
from datetime import datetime
from scrapers.cloudscraper_manager import Cloudscraper_Manager
from scrapers.scrapy_manager import Scrapy_Manager
from scrapers.selenium_scraper import Selenium_Scraper
from api.data_models import Session
from config.constants import *
from src.utils.logger import get_logger

class Scraper_Orchestrator:  
    def __init__(self):
        self.logger = get_logger("orchestrator")
        
        # Initialize scrapers in priority order
        self.scrapers = [
            Cloudscraper_Manager(), 
            Scrapy_Manager(), 
            Selenium_Scraper()
        ]
        
        # Simple rate limiting tracking
        self.last_request_time = 0
        self.min_delay = 1.0 / RATE_LIMIT 
        
        # Results storage
        self.results = {}

        self.finished = False
        
    def _apply_rate_limit(self):
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_delay:
            # Add some random jitter (0-20% extra delay)
            sleep_time = (self.min_delay - time_since_last) * (1 + random.random() * 0.2)
            self.logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _save_checkpoint(self, firm_name, page, reviews_count, reviews):
        checkpoint_file = CHECKPOINT_PATH / f"{firm_name}_checkpoint.json"

        serializable_reviews = []
        for review in reviews:
            if hasattr(review, 'to_dict'):
                serializable_reviews.append(review.to_dict())
            elif isinstance(review, dict):
                serializable_reviews.append(review)
            else:
                self.logger.warning(f"Unknown review type: {type(review)}")
        
        checkpoint_data = {
            'firm_name': firm_name,
            'last_page': page,
            'reviews_count': reviews_count,
            'reviews': serializable_reviews,
            'timestamp': datetime.now().isoformat()
        }
        
        try:
            with open(checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2)
            self.logger.debug(f"Saved checkpoint for {firm_name} at page {page}")
        except Exception as e:
            self.logger.error(f"Failed to save checkpoint: {e}")
    
    def _load_checkpoint(self, firm_name):
        checkpoint_file = CHECKPOINT_PATH / f"{firm_name}_checkpoint.json"
        
        if not checkpoint_file.exists():
            return None
        
        try:
            with open(checkpoint_file, 'r') as f:
                checkpoint = json.load(f)
            self.logger.info(f"Loaded checkpoint for {firm_name}: page {checkpoint['last_page']}")
            return checkpoint
        except Exception as e:
            self.logger.error(f"Failed to load checkpoint: {e}")
            return None
    
    def _clear_checkpoint(self, firm_name):
        checkpoint_file = CHECKPOINT_PATH / f"{firm_name}_checkpoint.json"
        if checkpoint_file.exists():
            checkpoint_file.unlink()
            self.logger.debug(f"Cleared checkpoint for {firm_name}")
    
    def scrape_firm(self, firm):
        url = FOREX_PROP_FIRMS.get(firm) or FUTURES_PROP_FIRMS.get(firm)
        if url is None:
            raise ValueError(f"Unknown firm: {firm}")
        self.logger.info(f"Starting scrape for {firm} at {url}")

        self._manage_proxies()
        firm_data = self._scrape_firm_info(url)
        
        # Try each scraper until one works
        for scraper in self.scrapers:
            self.logger.info(f"Trying {scraper.name} for {firm}")

            # Check for existing checkpoint
            checkpoint = self._load_checkpoint(firm)
            start_page = checkpoint['last_page'] + 1 if checkpoint else 1
            loaded_reviews = checkpoint['reviews'] if checkpoint else []

            result = self._scrape_with_scraper(scraper, firm, url, start_page, loaded_reviews, firm_data)

            # save after scraping
            if result and result.reviews:
                self._save_checkpoint(firm, result.total_pages, len(result.reviews), result.reviews)

            if result and (self.finished or isinstance(scraper, Selenium_Scraper)):
                return result
            
        self.logger.error(f"All scrapers failed for {firm}")
        return None
    
    def _scrape_with_scraper(self, scraper, firm, url, start_page, loaded_reviews, firm_data):
        try:
            self._apply_rate_limit()

            # Initialize result
            result = Session(
                firm_name=firm,
                trustpilot_url=url,
                firm_data=firm_data,
                reviews=loaded_reviews.copy(),
                scrape_date=datetime.now(),
                scraper_used=scraper.name,
                total_pages=start_page,
                success=False
            )
            
            # Scrape reviews page by page
            page = start_page
            consecutive_failures = 0
            max_pages = 1000  # Safety limit
            
            while page <= max_pages:
                self.logger.info(f"Scraping page {page} for {firm}")
                
                # Apply rate limiting
                self._apply_rate_limit()
                
                # Scrape the page
                try:
                    reviews, has_next = scraper.scrapes_reviews_page(url, page)
                    
                    if reviews:
                        result.reviews.extend(reviews)
                        consecutive_failures = 0
                        # Save checkpoint every 5 pages
                        if page % 5 == 0:
                            self._save_checkpoint(firm, page, len(result.reviews), result.reviews)
                        self.logger.info(f"Got {len(reviews)} reviews from page {page}")
                    else:
                        consecutive_failures += 1
                        self.logger.warning(f"No reviews found on page {page}")
                    if consecutive_failures >= 3:
                        self.logger.warning(f"Too many failures, stopping at page {page}")
                        break
                    if not has_next:
                        self.finished = True
                        self.logger.info(f"Reached last page ({page}) for {firm}")
                        break
                    page += 1
                except Exception as e:
                    self.logger.error(f"[{e}] Error on page {page}")
                    consecutive_failures += 1
                    if consecutive_failures >= 3:
                        break
                    
                    # Wait a bit longer after error
                    time.sleep(RETRY_DELAY)
            
            # Update final stats
            result.total_pages = page
            result.success = len(result.reviews) > 0
            
            if result.success:
                self.logger.info(f"Successfully scraped {firm}: {len(result.reviews)} reviews")

            if isinstance(scraper, Selenium_Scraper):
                self._clear_checkpoint(firm)

            return result
            
        except Exception as e:
            self.logger.error(f"Fatal error scraping {firm} with {scraper.name}: {e}")
            return None
    
    def scrape_all_firms(self, firms_dict=None):
        if firms_dict is None:
            firms_dict = {**FOREX_PROP_FIRMS, **FUTURES_PROP_FIRMS}
        self.logger.info(f"Starting scrape of {len(firms_dict)} firms")
        
        # Track overall progress
        total_start_time = datetime.now()
        successful_firms = 0
        total_reviews = 0
        firms_list = list(firms_dict.items())
        
        for i, (name, url) in enumerate(firms_list, 1):
            firm_start_time = datetime.now()
            
            self.logger.info(f"\n{'='*60}")
            self.logger.info(f"Processing firm {i}/{len(firms_list)}: {name}")
            self.logger.info(f"{'='*60}")
            
            result = self.scrape_firm(name)
            
            if result:
                self.results[name] = result
                self._save_result(result)
                
                successful_firms += 1
                total_reviews += len(result.reviews)
                
                # Log firm completion stats
                firm_duration = (datetime.now() - firm_start_time).total_seconds()
                self.logger.info(
                    f"Completed {name}: {len(result.reviews)} reviews "
                    f"in {firm_duration/60:.1f} minutes"
                )
            else:
                self.logger.error(f"Failed to scrape {name}")
            
            # Show overall progress
            elapsed = (datetime.now() - total_start_time).total_seconds()
            avg_time_per_firm = elapsed / i
            remaining_firms = len(firms_list) - i
            eta_seconds = avg_time_per_firm * remaining_firms
            
            self.logger.info(
                f"\nProgress: {i}/{len(firms_list)} firms | "
                f"Success rate: {successful_firms/i*100:.1f}% | "
                f"Total reviews: {total_reviews} | "
                f"ETA: {eta_seconds/60:.1f} minutes"
            )
        
        # Generate final summary
        self._generate_summary_report()
    
    def _scrape_firm_info(self, url):
         # only using cloudscraper (fastest & easiest)
        scraper = self.scrapers[0]
        if isinstance(scraper, Cloudscraper_Manager) :
            firm_data = scraper.scrapes_info(url)
            if not firm_data:
                self.logger.error(f"Failed to get firm information")
                return {}
            
            return firm_data

    def _manage_proxies(self):
        # Resurrect dead proxies periodically (every 5 firms)
        if hasattr(self, '_firms_scraped') and self._firms_scraped % 5 == 0:
            for scraper in self.scrapers:
                if hasattr(scraper, 'proxy_rotator'):
                    scraper.proxy_rotator.resurrect_proxies()
        
        # Track scraped firms
        if not hasattr(self, '_firms_scraped'):
            self._firms_scraped = 0    
        self._firms_scraped += 1
    
    def _save_result(self, result):
        try:
            safe_firm_name = result.firm_name.replace(" ", "_").replace("/", "_")
            output_file = PARSED_DATA_PATH / f"{safe_firm_name}.json"
        
            # Convert reviews to dictionaries safely
            serializable_reviews = []
            for review in result.reviews:
                if isinstance(review, dict):
                    serializable_reviews.append(review)
                elif hasattr(review, 'to_dict'):
                    serializable_reviews.append(review.to_dict())
                else:
                    self.logger.warning(f"Unknown review type: {type(review)}")
                    continue

            # Handle firm_data - convert Firm object to dict
            firm_data = result.firm_data
            if hasattr(firm_data, 'to_dict'):
                firm_data = firm_data.to_dict()
            elif hasattr(firm_data, '__dict__'):
                firm_data = firm_data.__dict__.copy()
            elif not isinstance(firm_data, dict):
                firm_data = {
                    attr: getattr(firm_data, attr) 
                    for attr in dir(firm_data) 
                    if not attr.startswith('_') and not callable(getattr(firm_data, attr))
            }

            final_data = {
                'firm_name': result.firm_name,
                'trustpilot_url': result.trustpilot_url,
                'firm_data': firm_data,
                'reviews': serializable_reviews, 
                'total_reviews': len(serializable_reviews),
                'scrape_date': result.scrape_date.isoformat() if hasattr(result.scrape_date, 'isoformat') else str(result.scrape_date),
                'scraper_used': result.scraper_used,
                'total_pages': result.total_pages,
                'success': result.success
            }

            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(final_data, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Saved results to {output_file}")
        except Exception as e:
            self.logger.error(f"Failed to save results: {e}")
    
    def _generate_summary_report(self):
        """Generate a summary report of the scraping session"""
        report = {
            'scrape_date': datetime.now().isoformat(),
            'total_firms_attempted': len(self.results),
            'successful_firms': sum(1 for r in self.results.values() if r.success),
            'total_reviews': sum(len(r.reviews) for r in self.results.values()),
            'scrapers_used': {},
            'firms': {}
        }
        
        # Count scraper usage
        report['scrapers_used'] = dict(Counter(result.scraper_used for result in self.results.values()))
        
        # Add per-firm summary
        for firm_name, result in self.results.items():
            report['firms'][firm_name] = {
                'success': result.success,
                'reviews_count': len(result.reviews),
                'pages_scraped': result.total_pages,
                'scraper_used': result.scraper_used,
                'rating': result.firm_data.rating if result.firm_data else None
            }
        
        # Save report
        report_file = PARSED_DATA_PATH / 'scraping_report.json'
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        # Print summary to console
        self.logger.info(f"\n{'='*60}")
        self.logger.info("SCRAPING SESSION COMPLETE")
        self.logger.info(f"{'='*60}")
        self.logger.info(f"Total firms: {report['total_firms_attempted']}")
        self.logger.info(f"Successful: {report['successful_firms']}")
        self.logger.info(f"Total reviews: {report['total_reviews']}")
        self.logger.info(f"Scrapers used: {report['scrapers_used']}")
        self.logger.info(f"Report saved to: {report_file}")
        self.logger.info(f"{'='*60}")