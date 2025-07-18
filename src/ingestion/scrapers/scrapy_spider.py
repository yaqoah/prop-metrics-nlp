import json
import random
from scrapy import Spider, Request
from parsers.trustpilot import Trustpilot
from bs4 import BeautifulSoup
from config.constants import USER_AGENTS

class Trustpilot_Spider(Spider):
    name = "trustpilot"
    
    def __init__(self, start_url=None, output_file=None, proxy_rotator=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_urls = [start_url] if start_url else []
        self.parser = Trustpilot()
        self.proxy_rotator = proxy_rotator
        self.data = {'firm_info': None, 'reviews': [], 'has_next': False}
        self.output_file = output_file
        
    def parse(self, response):
        soup = BeautifulSoup(response.text, 'html.parser')
        self.data['firm_info'] = self.parser.parse_firm(soup, response.url)
        
        # Parse reviews on first page
        reviews = self.parser.parse_reviews(soup)
        self.data['reviews'].extend(reviews)
        
        next_page = response.css('a[name="pagination-button-next"]::attr(href)').get()
        if next_page:
            yield self._make_request(response.urljoin(next_page), self.parse_reviews)
    
    def parse_reviews(self, response):
        soup = BeautifulSoup(response.text, 'html.parser')
        reviews = self.parser.parse_reviews(soup)
        self.data['reviews'].extend(reviews)
        
        # Follow pagination
        next_page = response.css('a[name="pagination-button-next"]::attr(href)').get()
        if next_page:
            yield self._make_request(response.urljoin(next_page), self.parse_reviews)
        else:
            self.data['has_next'] = False
    
    def handle_error(self, failure):
        request = failure.request
        
        if 'proxy_obj' in request.meta and self.proxy_rotator:
            self.proxy_rotator.mark_proxy_dead(request.meta['proxy_obj'])
            
        self.logger.error(f"Request failed: {failure.value}")
    
    def start_requests(self):
        for url in self.start_urls:
            yield self._make_request(url)
    
    def _make_request(self, url, callback=None, dont_filter=False):
        request = Request(
            url,
            callback=callback or self.parse,
            dont_filter=dont_filter,
            headers={'User-Agent': random.choice(USER_AGENTS)}
        )
        
        # Add proxy if available
        if self.proxy_rotator and self.proxy_rotator.enabled:
            proxy = self.proxy_rotator.get_proxy()
            if proxy:
                proxy_url = proxy.get('http', proxy.get('https'))
                request.meta['proxy'] = proxy_url
                
        return request

    def closed(self, reason):
        if self.output_file:
            with open(self.output_file, 'w') as f:
                json.dump(self.data, f, default=str)