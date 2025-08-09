import os
import socket
import psycopg2
from dotenv import load_dotenv
from database.schema import SCHEMA_SQL
from supabase import create_client
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import Json, execute_values
from pgvector.psycopg2 import register_vector
from contextlib import contextmanager
from src.utils.logger import get_logger
from urllib.parse import urlparse

class SupabaseConnection:
    def __init__(self):
        load_dotenv()
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_KEY")
        self.client = create_client(self.supabase_url, self.supabase_key)
        self.logger = get_logger("database")
        self.pool = None
        self.use_fallback = False

        # Try to establish PostgreSQL connection
        try:
            db_url = os.getenv("DATABASE_URL")
            parsed = urlparse(db_url)
            
            # Try different connection methods
            connection_attempts = [
                # Method 1: Try with original hostname (let psycopg2 handle resolution)
                {
                    "host": parsed.hostname,
                    "database": parsed.path[1:],
                    "user": parsed.username,
                    "password": parsed.password,
                    "port": parsed.port or 5432,
                    "connect_timeout": 10,
                    "sslmode": 'require'
                },
                # Method 2: Try with pooler endpoint if available
                {
                    "dsn": db_url + "?connect_timeout=10&sslmode=require"
                }
            ]
            
            for i, conn_params in enumerate(connection_attempts):
                try:
                    self.logger.info(f"Trying connection method {i+1}")
                    if "dsn" in conn_params:
                        self.pool = SimpleConnectionPool(1, 20, conn_params["dsn"])
                    else:
                        self.pool = SimpleConnectionPool(1, 20, **conn_params)
                    self.logger.info("PostgreSQL connection established successfully")
                    break
                except Exception as e:
                    self.logger.warning(f"Connection method {i+1} failed: {e}")
                    continue
                    
            if self.pool is None:
                raise Exception("All PostgreSQL connection methods failed")
                
        except Exception as e:
            self.logger.warning(f"PostgreSQL connection failed: {e}")
            self.logger.info("Using Supabase client as fallback - this may be slower")
            self.use_fallback = True

    @contextmanager
    def get_db_connection(self):
        if self.use_fallback:
            raise NotImplementedError("Direct SQL not available in fallback mode. Use Supabase client methods.")
        
        conn = self.pool.getconn()
        try:
            register_vector(conn)
            yield conn
        finally:
            self.pool.putconn(conn)

    def initialize_schema(self):
        if self.use_fallback:
            self.logger.warning("Schema initialization skipped in fallback mode")
            return
            
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(SCHEMA_SQL)
                    conn.commit()
        except Exception as e:
            self.logger.error(f"{e}: on running schema")
     
    def bulk_insert_reviews(self, reviews_data):
        if self.use_fallback:
            # Use Supabase client for insertion
            self.logger.info("Using Supabase client for bulk insert")
            
            # Process in batches for better performance
            batch_size = 1000
            for i in range(0, len(reviews_data), batch_size):
                batch = reviews_data[i:i+batch_size]
                
                # Prepare batch for Supabase client
                formatted_batch = []
                for review in batch:
                    formatted_review = {
                        "firm_name": review["firm_name"],
                        "review_id": review["review_id"],
                        "author_name": review.get("author_name"),
                        "rating": review.get("rating"),
                        "date_posted": review.get("date_posted"),
                        "content": review["content"],
                        "title": review.get("review_title"),
                        "is_valid": review.get("is_valid", True),
                        "validation_flags": review.get("validation_flags", {}),
                        "language": review.get("language"),
                        "sentiment_score": review.get("sentiment_score"),
                        "sentiment_label": review.get("sentiment_label"),
                        "emotion_scores": review.get("emotion_scores", {}),
                        "summary": review.get("summary"),
                        "entities": review.get("entities", []),
                        "aspects": review.get("aspects", []),
                        "key_phrases": review.get("key_phrases", []),
                        "primary_topic_id": review.get("primary_topic_id"),
                        "topic_distribution": review.get("topic_distribution", {})
                    }
                    
                    if review.get("embedding") is not None:
                        embedding = review["embedding"]     
                        if hasattr(embedding, 'tolist'):
                            embedding = embedding.tolist()
                        if isinstance(embedding, list):
                            embedding = [round(float(x), 6) for x in embedding]
                            formatted_review["embedding"] = embedding
                        else:
                            self.logger.warning(f"Skipping invalid embedding for review {review['review_id']}")
                            formatted_review["embedding"] = None
                    else:
                        formatted_review["embedding"] = None
                    
                    formatted_batch.append(formatted_review)
        
                try:
                    self.client.table('reviews').upsert(formatted_batch, on_conflict='review_id').execute()
                    self.logger.info(f"Inserted batch of {len(batch)} reviews")
                except Exception as e:
                    self.logger.error(f"Failed to insert batch: {e}")
                    for review in formatted_batch:
                        try:
                            if review.get("embedding") and len(str(review["embedding"])) > 50000:
                                self.logger.warning(f"Embedding too large for review {review['review_id']}, skipping embedding")
                                review["embedding"] = None    
                            self.client.table('reviews').upsert(review, on_conflict='review_id').execute()
                        except Exception as e2:
                            self.logger.error(f"Failed to insert review {review.get('review_id')}: {e2}")
            return
        
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
                        firm_name = EXCLUDED.firm_name,
                        author_name = EXCLUDED.author_name,
                        rating = EXCLUDED.rating,
                        date_posted = EXCLUDED.date_posted,
                        content = EXCLUDED.content,
                        title = EXCLUDED.title,
                        is_valid = EXCLUDED.is_valid,
                        validation_flags = EXCLUDED.validation_flags,
                        language = EXCLUDED.language,
                        sentiment_score = EXCLUDED.sentiment_score,
                        sentiment_label = EXCLUDED.sentiment_label,
                        emotion_scores = EXCLUDED.emotion_scores,
                        summary = EXCLUDED.summary,
                        entities = EXCLUDED.entities,
                        aspects = EXCLUDED.aspects,
                        key_phrases = EXCLUDED.key_phrases,
                        primary_topic_id = EXCLUDED.primary_topic_id,
                        topic_distribution = EXCLUDED.topic_distribution,
                        embedding = EXCLUDED.embedding,
                        processed_at = CURRENT_TIMESTAMP
                """

                values = []
                for review in reviews_data:
                    # For PostgreSQL with pgvector, embeddings can be passed directly
                    embedding_value = review.get("embedding")
                    
                    # Convert numpy array to list if needed
                    if embedding_value is not None and hasattr(embedding_value, 'tolist'):
                        embedding_value = embedding_value.tolist()
                    
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
                        embedding_value  # pgvector handles the conversion
                    ))

                execute_values(cur, insert_query, values)
                conn.commit()