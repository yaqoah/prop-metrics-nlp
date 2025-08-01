import os
import traceback
import pickle
from datetime import datetime
from pathlib import Path
from src.processing.pipeline.queue_manager import QueueManager
from src.processing.pipeline.validator import DataValidator
from src.processing.nlp.transformers_engine import TransformersEngine
from src.processing.nlp.spacy_extractor import SpacyExtractor
from src.processing.nlp.topic_modeler import TopicModeler
from src.processing.nlp.embeddings_generator import EmbeddingsGenerator
from src.utils.logger import get_logger
from database.connection import SupabaseConnection


class Orchestrator:
    def __init__(self):
        self.queue_manager = QueueManager()
        self.validator = DataValidator()
        self.transformer_engine = TransformersEngine()
        self.spacy_extractor = SpacyExtractor()
        self.topic_modeler = TopicModeler()
        self.generate_embeddings = EmbeddingsGenerator()
        self.db = SupabaseConnection()
        self.logger = get_logger("orchestrator")

        self.use_fallback = self.db.use_fallback
        if self.use_fallback:
            self.logger.info("Orchestrator operating in Supabase client fallback mode")

        checkpoint_dir = os.getenv("checkpoint_dir")
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.gpu_batch_size = 500 

        self.db.initialize_schema()
    
    def save_checkpoint(self, checkpoint_data):
        checkpoint_file = self.checkpoint_dir / f"{checkpoint_data['firm_name']}_checkpoint.pkl"
        with open(checkpoint_file, 'wb') as f:
            pickle.dump(checkpoint_data, f)
        self.logger.info(f"Checkpoint saved for {checkpoint_data['firm_name']} at stage {checkpoint_data['stage']}")
    
    def load_checkpoint(self, firm_name):
        checkpoint_file = self.checkpoint_dir / f"{firm_name}_checkpoint.pkl"
        if checkpoint_file.exists():
            with open(checkpoint_file, 'rb') as f:
                checkpoint = pickle.load(f)
                self.logger.info(f"Loaded checkpoint for {firm_name} at stage {checkpoint['stage']}")
                return checkpoint
        return None
    
    def delete_checkpoint(self, firm_name):
        checkpoint_file = self.checkpoint_dir / f"{firm_name}_checkpoint.pkl"
        if checkpoint_file.exists():
            checkpoint_file.unlink()
            self.logger.info(f"Deleted checkpoint for {firm_name}")
    
    def process_firm_reviews(self, firm_name, reviews):
        self.logger.info(f"Starting {firm_name}'s full processing of {len(reviews)} reviews")
        
        checkpoint = self.load_checkpoint(firm_name)
        if checkpoint:
            valid_reviews = checkpoint.get('valid_reviews', None)
            stage = checkpoint['stage']
            processed_batches = checkpoint.get('processed_batches', 0)
            topic_data = checkpoint.get('topic_data', None)
        else:
            valid_reviews = None
            stage = 'validation'
            processed_batches = 0
            topic_data = None

        try:
            # 1. validate reviews
            if stage == 'validation':
                self.logger.info(f"Stage 1: Validating {len(reviews)} reviews")
                valid_reviews = self.validator.validate_batch(reviews)
                
                self.save_checkpoint({
                    'firm_name': firm_name,
                    'stage': 'nlp_processing',
                    'valid_reviews': valid_reviews,
                    'processed_batches': 0
                })
                stage = 'nlp_processing'
            
            # 2. nlp
            if stage == 'nlp_processing':
                self.logger.info(f"Stage 2: NLP processing from batch {processed_batches}")
                
                for i in range(processed_batches * self.gpu_batch_size, len(valid_reviews), self.gpu_batch_size):
                    batch = valid_reviews[i:i+self.gpu_batch_size]
                    
                    self.logger.info(f"Processing batch {i//self.gpu_batch_size + 1}/{(len(valid_reviews)-1)//self.gpu_batch_size + 1}")
                    
                    batch = self.transformer_engine.process_batch(batch)
                    batch = self.spacy_extractor.process_batch(batch)
                    
                    valid_reviews[i:i+self.gpu_batch_size] = batch
                    
                    if (i // self.gpu_batch_size + 1) % 2 == 0 or i + self.gpu_batch_size >= len(valid_reviews):
                        self.save_checkpoint({
                            'firm_name': firm_name,
                            'stage': 'nlp_processing' if i + self.gpu_batch_size < len(valid_reviews) else 'topic_modeling',
                            'valid_reviews': valid_reviews,
                            'processed_batches': i // self.gpu_batch_size + 1
                        })
                
                stage = 'topic_modeling'
            
            # 3. topic modeling reviews
            if stage == 'topic_modeling':
                self.logger.info("Stage 3: Topic modeling")
                topic_data = self.topic_modeler.fit_transform(valid_reviews)
                valid_reviews = self.topic_modeler.update_reviews_with_topics(valid_reviews, topic_data)
                
                # store topics
                if topic_data.get("topics"):
                    self._store_topics(firm_name, topic_data["topics"])
                
                self.save_checkpoint({
                    'firm_name': firm_name,
                    'stage': 'embeddings',
                    'valid_reviews': valid_reviews,
                    'topic_data': topic_data,
                    'processed_batches': 0
                })
                stage = 'embeddings'
            
            # 4. embeddings
            if stage == 'embeddings':
                self.logger.info(f"Stage 4: Generating embeddings from batch {processed_batches}")
                
                for i in range(processed_batches * self.gpu_batch_size, len(valid_reviews), self.gpu_batch_size):
                    batch = valid_reviews[i:i+self.gpu_batch_size]
                    batch = self.generate_embeddings.process_batch(batch)
                    valid_reviews[i:i+self.gpu_batch_size] = batch
                    
                    if (i // self.gpu_batch_size + 1) % 2 == 0:
                        self.save_checkpoint({
                            'firm_name': firm_name,
                            'stage': 'embeddings',
                            'valid_reviews': valid_reviews,
                            'topic_data': topic_data,
                            'processed_batches': i // self.gpu_batch_size + 1
                        })
            
            # 5. Store reviews
            self.logger.info("Stage 5: Storing reviews in database")
            self._store_reviews(valid_reviews)
            
            # self.delete_checkpoint(firm_name)
            
            self.logger.info(f"Finished {firm_name}'s full processing: resulting in"
                           f" {len(valid_reviews)} / {len(reviews)} valid processed reviews")
                           
        except Exception as e:
            self.logger.error(f"Error during {stage} stage: {e}")
            raise


    def _store_topics(self, firm_name, topics):
        if self.use_fallback:
            for topic in topics:
                try:
                    self.db.client.table('topics').insert({
                        'firm_name': firm_name,
                        'topic_name': topic.get("topic_name"),
                        'keywords': topic.get("keywords"),
                        'representative_docs': topic.get("representative_docs")
                    }).execute()
                except Exception as e:
                    self.logger.error(f"Error storing topic via Supabase client: {e}")
        else:
            with self.db.get_db_connection() as conn:
                with conn.cursor() as cur:
                    for topic in topics:
                        cur.execute("""  # HERE FIX: Changed excecute to execute
                            INSERT INTO topics (firm_name, topic_name, keywords, representative_docs)
                            VALUES (%s, %s, %s, %s)
                        """, (
                            firm_name,
                            topic["topic_name"],
                            topic["keywords"],
                            topic["representative_docs"]
                        ))
                    conn.commit()
    
    def _store_reviews(self, reviews):
        review_data = []
        for review in reviews:
            review["processed_at"] = datetime.now()
            review_data.append(review)

        self.db.bulk_insert_reviews(review_data)
    
    def run_pipeline(self):
        self.logger.info("Starting data processing pipeline ~")

        import torch
        self.logger.info(f"GPU Available: {torch.cuda.is_available()}")
        if torch.cuda.is_available():
            self.logger.info(f"GPU Device: {torch.cuda.get_device_name(0)}")
            
        firms_data = {}
        for batch_info in self.queue_manager.get_review_batches():
            firm_name = batch_info["firm_name"]
    
            if firm_name not in firms_data:
                firms_data[firm_name] = []
            
            firms_data[firm_name].extend(batch_info["reviews"])

        firms_to_process = []
        for firm_name in firms_data.keys():
            checkpoint = self.load_checkpoint(firm_name)
            if checkpoint:
                firms_to_process.insert(0, firm_name) 
            else:
                firms_to_process.append(firm_name)

        for firm_name in firms_to_process:
            try:
                self.process_firm_reviews(firm_name, firms_data[firm_name])
            except Exception as e:
                self.logger.error(f"[{e}] Error on processing {firm_name}'s data"
                                  f"\n{traceback.format_exc()}")
                continue
                
        self.logger.info("Pipeline processing completed")

        