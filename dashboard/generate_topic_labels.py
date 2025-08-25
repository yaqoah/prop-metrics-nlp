import os
import re
import time
import psycopg2
import argparse
from litellm import completion 
from database.connection import SupabaseConnection
from tqdm import tqdm
import pandas as pd

def get_clean_topic_label(keywords: str, retries=2) -> str:
    
    cleaned_keywords = re.sub(r'^\d+_', '', keywords).replace('_', ' ')
    if not cleaned_keywords.strip():
        return "General Feedback"

    for attempt in range(retries + 1):
        try:
            response = completion(
                model="ollama/mistral", 
                messages=[
                    {
                        "role": "system", 
                        "content": "You are a helpful AI assistant that specializes in summarizing keywords from customer reviews into a concise, human-readable topic title. Your response should ONLY be the title itself, in Title Case, and between 2-4 words. Do not use quotes."
                    },
                    # Example 1: Show the model what to do
                    {
                        "role": "user",
                        "content": "Keywords: 'service good service service great service good'"
                    },
                    {
                        "role": "assistant",
                        "content": "Excellent Customer Service"
                    },
                    # Example 2: Another example
                    {
                        "role": "user",
                        "content": "Keywords: 'platform mt5 mt4 platform mt5'"
                    },
                    {
                        "role": "assistant",
                        "content": "Trading Platform (MT4/MT5)"
                    },
                    # The actual request for the current keywords
                    {
                        "role": "user",
                        "content": f"Keywords: '{cleaned_keywords}'"
                    }
                ],
                temperature=0.1,
                max_tokens=20,
                stream=False,
                logger_fn=None
            )

            label = response.choices[0].message.content.strip().strip('"')
            print(f"Raw: '{keywords}'  --->  Clean: '{label}'")
            tqdm.write(f"Raw: '{keywords}'  --->  Clean: '{label}'")
            return label
        
        except Exception as e:
            if attempt < retries:
                print(f"Retry {attempt+1}/{retries} for '{keywords}' due to error: {e}")
                time.sleep(3)
            else:
                tqdm.write(f"Error generating label for '{keywords}': {e}")
                return cleaned_keywords.title()

def update_batch(db, batch_data):
    """Helper function to update a batch."""
    try:
        with db.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    "UPDATE topics SET display_name = %s WHERE id = %s",
                    batch_data
                )
            conn.commit()
        return True
    except Exception as e:
        tqdm.write(f"❌ Failed to update batch: {e}")
        return False

def main(force_all: bool):
    """Main function to generate labels."""
    print("Connecting to the database...")
    db = SupabaseConnection()
    BATCH_SIZE = 25

    try:
        with db.get_db_connection() as conn:
            if force_all:
                print("--- FORCE ALL MODE: Re-processing all topics. ---")
                query = "SELECT id, topic_name FROM topics"
            else:
                print("--- Standard Mode: Processing only new topics. ---")
                query = "SELECT id, topic_name FROM topics WHERE display_name IS NULL OR display_name = ''"
            
            df = pd.read_sql(query, conn)
        
        if df.empty:
            print("No topics to label. All are up to date.")
            print("To re-process all topics, run this script with the --force-all flag.")
            return

        total_topics = len(df)
        print(f"Found {total_topics} topics to label. Starting process...")
        
        current_batch = []
        for row in tqdm(df.itertuples(), total=total_topics, desc="Labeling Topics"):
            display_name = get_clean_topic_label(row.topic_name)
            current_batch.append((display_name, row.id))
            
            if len(current_batch) >= BATCH_SIZE or row.Index + 1 == total_topics:
                if update_batch(db, current_batch):
                    pass
                current_batch = []

    except Exception as main_error:
        print(f"Fatal error in main process: {main_error}")
    finally:
        print("\nLabeling process complete.")


if __name__ == "__main__":
    # Setup to handle the --force-all command-line argument
    parser = argparse.ArgumentParser(description="Generate clean display names for topics.")
    parser.add_argument(
        "--force-all",
        action="store_true",
        help="If set, re-processes all topics in the database, not just new ones."
    )
    args = parser.parse_args()
    
    main(force_all=args.force_all)