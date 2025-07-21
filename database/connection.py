import os
import psycopg2
from schema import SCHEMA_SQL
from supabase import create_client
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import Json, execute_values
from pgvector.psycopg2 import register_vector
from contextlib import contextmanager
from src.utils.logger import get_logger

class SupabaseConnection:
    def __init__(self):
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_KEY")
        self.client = create_client(self.supabase_url, self.supabase_key)

        self.db_url = os.getenv("DATABASE_URL")
        self.pool = SimpleConnectionPool(1, 20, self.db_url)

        self.logger = get_logger("database")
    
    @contextmanager
    def get_db_connection(self):
        conn = self.pool.getconn()
        try:
            register_vector(conn)
            yield conn
        finally:
            self.pool.putconn(conn)

    def initialize_database(self):
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(SCHEMA_SQL)
                    conn.commit()
        except Exception as e:
            self.logger.error(f"[{e}] Error: DB not initialized, check schema.")
     
    def bulk_insert_reviews(self, reviews_data):
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                insert_query = """
                    INSERT INTO reviews (
                        firm_name, review_id, author_name, rating, date_posted,
                        content, title, is_valid, validation_flags,
                        language, sentiment_score, sentiment_label, emotion_scores,
                        summary, entities, aspects, key_phrases, primary_topic_id,
                        topic_distribution, embedding
                    ) VALUES %s
                    ON CONFLICT (review_id) DO UPDATE SET
                        processed_at = CURRENT_TIMESTAMP,
                        processing_version = EXCLUDED.processing_version
                """

                values = []
                for review in reviews_data:
                    values.append((
                        review["firm_name"],
                        review["review_id"],
                        review.get("author_name"),
                        review.get("rating"),
                        review.get("date_posted"),
                        review["content"],
                        review.get("review_title"),
                        review.get("is_valid", True),
                        Json(review.get("validation_flags", {})),
                        review.get("language"),
                        review.get("sentiment_score"),
                        review.get("sentiment_label"),
                        Json(review.get("emotion_scores", {})),
                        review.get("summary"),
                        Json(review.get("entities", [])),
                        Json(review.get("aspects", [])),
                        Json(review.get("key_phrases", [])),
                        review.get("primary_topic_id"),
                        Json(review.get("topic_distribution", {})),
                        review.get("embedding")
                    ))

                execute_values(cur, insert_query, values)
                conn.commit()
