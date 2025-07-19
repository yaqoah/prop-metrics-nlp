import json
import uuid
from pathlib import Path
from datetime import datetime
from src.utils.logger import get_logger
from src.ingestion.config.constants import PARSED_DATA_PATH
from database.connection import SupabaseConnection


class QueueManager:
    def __init__(self, batch_size=100):
        self.batch_size = batch_size
        self.db = SupabaseConnection()

        self.logger = get_logger("queue manager")

    def load_firm_data(self, firm):
        try:
            with open(firm, "r", encoding="utf-8") as f:
                data = json.load(f)

            firm_name, reviews = data.get("firm_name", []), []
            for review in data.get("reviews", []):
                review["firm_name"] = firm_name
                review["review_id"] = f"{firm_name}_{review.get("review_id", uuid.uuid4().hex)}"
                reviews.append(review)
    
            self.logger.info(f"Load {len(reviews)} reviews from {firm_name}")
            return firm_name, reviews
        
        except Exception as e:
            self.logger.error(f"Error Loading {firm_name}'s file - {e}")
            return "", []

    def create_processing_batch(self, firm_name, total_reviews):
        batch_id = f"{firm_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        try:
            with self.db.get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO processing_queue 
                        (batch_id, firm_name, total_reviews, status)
                        VALUES (%s, %s, %s, %s)
                    """, (batch_id, firm_name, total_reviews, "processing"))
                    conn.commit() 
        except Exception as e:
            self.logger.error(f"Error {e} on creating processing_queue")

    def create_batches(self, reviews):
        for i in range(0, len(reviews), self.batch_size):
            yield reviews[i:i + self.batch_size]

    def update_batch_progress(self, batch_id, processed_count):
        with self.db.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE processing_queue 
                    SET processed_reviews = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE batch_id = %s
                """, (processed_count, batch_id))
                conn.commit()

    def get_review_batches(self):
        firms = list(PARSED_DATA_PATH.glob("*.json"))

        for firm in firms:
            firm_name, firm_reviews = self.load_firm_data(firms)
            if not firm_reviews:
                continue
            
            batch_id = self.create_processing_batch(firm_name, len(firm_reviews))
            processed_count = 0
            for batch in self.create_batches(firm_reviews):
                yield {
                    "batch_id": batch_id,
                    "firm_name": firm_name,
                    "reviews": batch,
                    "batch_number": processed_count // self.batch_size + 1
                }
            
                processed_count += len(batch)
                self.update_batch_progress(batch_id, processed_count)
