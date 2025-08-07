from dotenv import load_dotenv
from pathlib import Path
import os

load_dotenv()

# Prop Firms
FOREX_PROP_FIRMS = {
    "FTMO": "https://www.trustpilot.com/review/ftmo.com",
    "FundingTraders": "https://www.trustpilot.com/review/fundingtraders.com",
    "FundedNext": "https://www.trustpilot.com/review/fundednext.com", 
    "AlphaCapital": "https://www.trustpilot.com/review/alphacapitalgroup.uk",
    "FundingPips": "https://www.trustpilot.com/review/fundingpips.com"
}

FUTURES_PROP_FIRMS = {
    "MyFundedFutures": "https://www.trustpilot.com/review/myfundedfutures.com",
    "TopStep": "https://www.trustpilot.com/review/topstep.com",
    "ApexTraderFunding": "https://www.trustpilot.com/review/apextraderfunding.com",
    "Tradeify": "https://www.trustpilot.com/review/tradeify.co",
    "TopOneFutures": "https://www.trustpilot.com/review/toponefutures.com"
}

# Scraping Configuration
RATE_LIMIT=1.5
REQUEST_TIMEOUT=30
MAX_RETRIES=3
RETRY_DELAY=5
CONCURRENT_REQUESTS=2

 # Proxy Configuration
IP_CHECK_URL = "https://www.trustpilot.com/"
PROXY_TEST_TIMEOUT = 2
PROXY_ENABLED = True
PROXY_LIST_PATH = Path("servers/proxies.txt")
# VALID_PROXY_LIST_PATH = Path("src/config/servers/valid_proxies.txt")

# Selenium Configuration
SELENIUM_HEADLESS=True
SELENIUM_DRIVER_PATH="C:/Users/Balot/chromedriver.exe"
SELENIUM_MAX_DRIVERS=3
SELENIUM_WAIT_TIMEOUT=20
CHROME="C:/Program Files (x86)/Google/Chrome/Application/chrome.exe"

# Data Storage
RAW_DATA_PATH=Path("./config/data/raw")
PARSED_DATA_PATH=Path("./src/ingestion/config/data/parsed")
CHECKPOINT_PATH=Path("./config/data/checkpoints")

# Logging
LOG_LEVEL="INFO"
LOG_FILE_PATH=Path("./src/utils/logs/analytics.log")
LOG_MAX_SIZE=10485760
LOG_BACKUP_COUNT=5

# User Agents 
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0"
]