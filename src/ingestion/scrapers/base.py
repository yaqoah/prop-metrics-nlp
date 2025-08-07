from abc import ABC, abstractmethod
from utils.constants import RATE_LIMIT, USER_AGENTS
import time
from datetime import datetime
import random
from src.utils.logger import get_logger 
from utils.constants import USER_AGENTS 
from middleware.proxy_rotator import Proxy_Rotator

class Base(ABC):
    def __init__(self, name):
        self.name = name
        self.requests = 0
        self.last_request_time = 0
        self.logger = get_logger(f"scraper.{name}")
        self.session_start = datetime.now()
        self.proxy_rotator = Proxy_Rotator()
        self.session_failed_proxies = set()

    @abstractmethod
    def scrapes_reviews_page(self, url: str, page: int) -> bool:
        pass
    
    def rate_limit(self):
        self.requests += 1
        current_time = time.time()
        from_last = current_time - self.last_request_time

        # create & randomise delay
        min_delay = 1.0 / RATE_LIMIT
        if from_last < min_delay:
            sleep_time = min_delay - from_last
            sleep_time *= (1 + random.random() * 0.2)  # 0%-20% jitter
            self.logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f}s")
            time.sleep(sleep_time)

        self.last_request_time = time.time()

    def get_headers(self):
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1"
        }
        headers["User-Agent"] = self.get_random_user_agent()

        return headers
    
    def get_random_user_agent(self):
        return random.choice(USER_AGENTS)
    
    def get_proxy(self):
        proxy = self.proxy_rotator.get_proxy()

        while proxy and str(proxy) in self.session_failed_proxies:
            proxy = self.proxy_rotator.get_proxy()
        
        return proxy
    
    def mark_proxy_failed(self, proxy):
        if proxy:
            self.session_failed_proxies.add(proxy)
            self.proxy_rotator.mark_proxy_dead(proxy)
            self.logger.warning(f"[{proxy}] Proxy failed, rotating to the next")