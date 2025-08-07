from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import time
from bs4 import BeautifulSoup
from .base import Base
from parsers.trustpilot import Trustpilot
from utils.constants import *
from .selenium_pool import Selenium_Drivers_Pool

class Selenium_Scraper(Base):
    
    def __init__(self):
        super().__init__("Selenium")
        self.driver_pool = Selenium_Drivers_Pool(SELENIUM_MAX_DRIVERS, self.proxy_rotator)
        self.parser = Trustpilot()
    
    def _wait_for_reviews(self, driver):
        wait = WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT)
        wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, "[data-service-review-card-paper]")
        ))
    
    def scrapes_info(self, url):
        driver, proxy = self._get_driver_with_proxy()
    
        try:
            driver.get(url)
            
            # Wait for main content to load
            wait = WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, 'h1')))
            
            # Wait a bit for all elements to render
            time.sleep(2)
            
            # Get page source and parse
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            firm_data = self.parser.parse_firm(soup, url)
            
            return firm_data
        except TimeoutException:
            self.logger.error(f"Timeout loading firm page: {url}")
            if proxy:
                self.mark_proxy_failed(proxy)
            return None
        except Exception as e:
            self.logger.error(f"Error scraping firm info: {e}")
            if proxy:
                self.mark_proxy_failed(proxy)
            return None
        finally:
            driver.quit()

    def can_handle_url(self, url):
        return True 
    
    def _get_driver_with_proxy(self):
        for attempt in range(MAX_RETRIES):
            proxy = self.get_proxy()
            try:
                driver = self.driver_pool._create_driver(proxy)
                driver.set_page_load_timeout(30)
                driver.get("https://www.trustpilot.com")
                
                return driver, proxy
                
            except Exception as e:
                self.logger.error(f"Driver creation failed: {e}")
                if proxy:
                    self.mark_proxy_failed(proxy)
                if 'driver' in locals():
                    driver.quit()

        return self.driver_pool._create_driver(None), None
    
    def scrapes_reviews_page(self, url, page):
        driver, proxy = self._get_driver_with_proxy()
        
        try:
            page_url = f"{url}?page={page}" if page > 1 else url
            driver.get(page_url)
            
            # Wait for reviews to load
            wait = WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT)
            try:
                # Wait for review cards to be present
                wait.until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, '[data-service-review-card-paper]')
                ))
            except TimeoutException:
                self.logger.warning(f"No reviews found on page {page}")
                return [], False
            
            # Scroll to load all lazy-loaded content
            last_height = driver.execute_script("return document.body.scrollHeight")
            scroll_attempts = 0
            max_scroll_attempts = 10
            
            while scroll_attempts < max_scroll_attempts:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                
                # Wait for new content to load
                time.sleep(1.5)
                
                # Calculate new scroll height and compare with last scroll height
                new_height = driver.execute_script("return document.body.scrollHeight")
                
                if new_height == last_height:
                    # Try scrolling to specific review cards to trigger lazy loading
                    review_cards = driver.find_elements(By.CSS_SELECTOR, '[data-service-review-card-paper]')
                    if review_cards:
                        # Scroll to last review card
                        driver.execute_script("arguments[0].scrollIntoView(true);", review_cards[-1])
                        time.sleep(1)
                    
                    # Check one more time
                    new_height = driver.execute_script("return document.body.scrollHeight")
                    if new_height == last_height:
                        break
                        
                last_height = new_height
                scroll_attempts += 1
            
            # Additional wait to ensure all content is rendered
            time.sleep(1)
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # Parse reviews using the parser
            reviews = self.parser.parse_reviews(soup)
            
            # Check if there's a next page
            has_next = False
            
            # Method 1: Check for next button
            try:
                next_button = driver.find_element(By.CSS_SELECTOR, 'a[name="pagination-button-next"]')
                # Check if button is not disabled
                if next_button and 'disabled' not in next_button.get_attribute('class'):
                    has_next = True
            except:
                pass
            
            # Method 2: Check pagination text (e.g., "Page 1 of 50")
            if not has_next:
                try:
                    pagination_text = driver.find_element(By.CSS_SELECTOR, '[data-pagination-summary]').text
                    import re
                    match = re.search(r'Page (\d+) of (\d+)', pagination_text)
                    if match:
                        current_page = int(match.group(1))
                        total_pages = int(match.group(2))
                        has_next = current_page < total_pages
                except:
                    pass
            
            # Method 3: Use parser's has_next_page method
            if not has_next:
                has_next = self.parser.has_next_page(soup)
            
            self.logger.info(f"Page {page}: Scraped {len(reviews)} reviews (has_next={has_next})")
            
            return reviews, has_next
            
        except TimeoutException as e:
            self.logger.error(f"Timeout on page {page}: {e}")
            if proxy:
                self.mark_proxy_failed(proxy)
            return [], False
            
        except WebDriverException as e:
            self.logger.error(f"WebDriver error on page {page}: {e}")
            if proxy:
                self.mark_proxy_failed(proxy)
            return [], False
            
        except Exception as e:
            self.logger.error(f"Unexpected error on page {page}: {e}")
            if proxy:
                self.mark_proxy_failed(proxy)
            return [], False
            
        finally:
            try:
                driver.quit()
            except:
                pass  # Driver might already be closed