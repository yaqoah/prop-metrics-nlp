import random
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from contextlib import contextmanager
from queue import Queue
from config.constants import *

class Selenium_Drivers_Pool():
    def __init__(self, max_drivers, proxy_rotator):
        self.max_drivers = max_drivers or SELENIUM_MAX_DRIVERS
        self.proxy_rotator = proxy_rotator
        self.drivers = Queue(maxsize=self.max_drivers)
    
    def _create_driver(self, proxy):
        options = Options()

        # Force to use your Chrome 138 binary
        options.binary_location = CHROME
        
        if SELENIUM_HEADLESS:
            options.add_argument("--headless")
        
        # Performance and stealth options
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-gpu")
        options.add_argument(f"user-agent={random.choice(USER_AGENTS)}")
        options.add_argument(f"--proxy-server={proxy}")
        prefs = {"profile.managed_default_content_settings.images": 2}
        options.add_experimental_option("prefs", prefs)

        # Anti-detection options
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        # Proxy configuration
        if proxy:
            proxy_url = proxy.get('http', proxy.get('https'))
            # Extract host:port from full URL
            if '://' in proxy_url:
                proxy_url = proxy_url.split('://', 1)[1]
            options.add_argument(f'--proxy-server={proxy_url}')
        
        if SELENIUM_DRIVER_PATH:
            service = Service(executable_path=SELENIUM_DRIVER_PATH)
            return webdriver.Chrome(service=service, options=options)
        else:
            return webdriver.Chrome(options=options)
    