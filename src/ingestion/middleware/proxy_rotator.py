import requests
import random
import time
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed

from config.constants import *
from src.utils.logger import get_logger
from pathlib import Path

class Proxy_Rotator:
    def __init__(self):
        self.logger = get_logger("proxy_rotator")
        self.enabled = PROXY_ENABLED
        self.proxies = []
        self.dead_proxies = []
        self.current_index = 0
        self.lock = Lock()
        
        if self.enabled:
            self._load_proxies()
            self._validate_proxies()
    
    def _load_proxies(self):
        BASE_DIR = Path(__file__).resolve().parent.parent
        VALID_PROXY_LIST_PATH = BASE_DIR / "config" / "valid_proxies.txt"

        if not VALID_PROXY_LIST_PATH.exists():
            self.logger.warning(f"Proxy file not found: {VALID_PROXY_LIST_PATH}")
            self.enabled = False
            return
        
        with open(VALID_PROXY_LIST_PATH, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    # Format: protocol://user:pass@host:port or protocol://host:port
                    parts = line.split('://')
                    if len(parts) == 2:
                        protocol, rest = parts
                        proxy = {
                            'http': line,
                            'https': line
                        }
                        self.proxies.append(proxy)
        
        self.logger.info(f"Loaded {len(self.proxies)} proxies")
    
    def _validate_proxies(self):
        self.logger.info("Validating proxies...")
        valid_proxies = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_proxy = {executor.submit(self._test_proxy, proxy): proxy for proxy in self.proxies}
            for future in as_completed(future_to_proxy):
                proxy = future_to_proxy[future]
                try:
                    if future.result():
                        valid_proxies.append(proxy)
                    else:
                        self.dead_proxies.append(proxy)
                except Exception as e:
                    self.logger.debug(f"Proxy check raised exception: {e}")
                    self.dead_proxies.append(proxy)
        
        self.proxies = valid_proxies
        self.logger.info(f"{len(self.proxies)} valid proxies, {len(self.dead_proxies)} dead")
        
        if not self.proxies:
            self.logger.error("No valid proxies found, disabling proxy rotation")
            self.enabled = False
    
    def _test_proxy(self, proxy):
        try:
            start = time.time()
            response = requests.get(
                IP_CHECK_URL,
                proxies=proxy,
                timeout=(2, 3)
            )
            latency = time.time() - start
            if response.status_code == 200 and latency < 1.5:
                self.logger.debug(f"Proxy {proxy} OK with latency {latency:.2f}s")
                return True
            else:
                self.logger.debug(f"Proxy {proxy} too slow: {latency:.2f}s")
                return False
        except Exception as e:
            self.logger.debug(f"Proxy test failed: {e}")
            return False
    
    def get_proxy(self):
        if not self.enabled or not self.proxies:
            return None
        
        with self.lock:
            proxy = random.choice(self.proxies)
            while proxy == getattr(self, 'last_proxy', None) and len(self.proxies) > 1:
                proxy = random.choice(self.proxies)
            self.last_proxy = proxy
            return proxy
    
    def mark_proxy_dead(self, proxy):
        with self.lock:
            if proxy in self.proxies:
                self.proxies.remove(proxy)
                self.dead_proxies.append(proxy)
                self.logger.warning(f"Marked proxy as dead. {len(self.proxies)} remaining")
                
                if not self.proxies:
                    self.logger.error("All proxies dead, disabling proxy rotation")
                    self.enabled = False
    
    def resurrect_proxies(self):
        if not self.dead_proxies:
            return
        
        self.logger.info(f"Testing {len(self.dead_proxies)} dead proxies...")
        resurrected = []
        
        for proxy in self.dead_proxies[:]:
            if self._test_proxy(proxy):
                self.dead_proxies.remove(proxy)
                resurrected.append(proxy)
        
        if resurrected:
            with self.lock:
                self.proxies.extend(resurrected)
                self.enabled = True
                self.logger.info(f"Resurrected {len(resurrected)} proxies")