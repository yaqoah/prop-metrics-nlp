#!/usr/bin/env python3
import argparse
import sys
from datetime import datetime
import json
import pandas as pd

from scrapers_orchestrator import Scraper_Orchestrator
from config.constants import FOREX_PROP_FIRMS, FUTURES_PROP_FIRMS, PARSED_DATA_PATH
from utils.logger import get_logger

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Extract prop trading firm reviews from Trustpilot"
    )
    
    parser.add_argument(
        '--category',
        choices=['forex', 'futures', 'all'],
        default='all',
        help='Category of firms to scrape'
    )
    
    parser.add_argument(
        '--firms',
        nargs='+',
        help='Specific firm names to scrape'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        help='Maximum number of firms to scrape'
    )
    
    parser.add_argument(
        '--export-csv',
        action='store_true',
        help='Export results to CSV after scraping'
    )
    
    return parser.parse_args()

def get_firms_to_scrape(args):
    if args.firms:
        # Filter specific firms from both dictionaries
        firms = {}
        for firm_name in args.firms:
            if firm_name in FOREX_PROP_FIRMS:
                firms[firm_name] = FOREX_PROP_FIRMS[firm_name]
            elif firm_name in FUTURES_PROP_FIRMS:
                firms[firm_name] = FUTURES_PROP_FIRMS[firm_name]
            else:
                print(f"Warning: Firm '{firm_name}' not found")
        return firms
    
    # Get firms by category
    if args.category == 'forex':
        firms = FOREX_PROP_FIRMS.copy()
    elif args.category == 'futures':
        firms = FUTURES_PROP_FIRMS.copy()
    else:  # 'all'
        firms = {**FOREX_PROP_FIRMS, **FUTURES_PROP_FIRMS}
    
    # Apply limit if specified
    if args.limit:
        firms = dict(list(firms.items())[:args.limit])
    
    return firms

def export_to_csv(output_dir):
    all_reviews = []
    
    # Read all JSON files and collect reviews
    for json_file in output_dir.glob("*.json"):
        if json_file.name == "scraping_report.json":
            continue
            
        with open(json_file, 'r') as f:
            data = json.load(f)
            
        firm_name = data['firm_name']
        category = data['category']
        
        for review in data['reviews']:
            all_reviews.append({
                'firm_name': firm_name,
                'category': category,
                'rating': review['rating'],
                'title': review['title'],
                'content': review['content'],
                'author': review['author_name'],
                'date': review['date_posted'],
                'verified': review.get('verified', False)
            })
    
    # Save to CSV
    if all_reviews:
        df = pd.DataFrame(all_reviews)
        csv_file = output_dir / f"reviews_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(csv_file, index=False)
        print(f"\nExported {len(all_reviews)} reviews to {csv_file}")

def main():
    args = parse_arguments()
    logger = get_logger("main")
    
    # Get firms to scrape
    firms = get_firms_to_scrape(args)
    
    if not firms:
        logger.error("No firms to scrape")
        return
    
    logger.info(f"Starting to scrape {len(firms)} firms")
    
    # Create orchestrator and run scraping
    orchestrator = Scraper_Orchestrator()
    
    try:
        # Start scraping
        start_time = datetime.now()
        orchestrator.scrape_all_firms(firms)
        
        # Print summary
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"\n Scraping Completed in {duration/60:.1f} minutes")
        
        # Export to CSV if requested
        if args.export_csv:
            export_to_csv(PARSED_DATA_PATH)
        
        return
        
    except KeyboardInterrupt:
        logger.info("\nScraping interrupted by user")
        return 
    except Exception as e:
        logger.error(f"[{e}] ERROR: SCRAPING FAILED")
        return 

if __name__ == "__main__":
    sys.exit(main())