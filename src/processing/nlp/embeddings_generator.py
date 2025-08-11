import torch
import numpy as np
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
from src.utils.logger import get_logger


class EmbeddingsGenerator:
    def __init__(self):
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model = SentenceTransformer("all-MiniLM-L6-v2", device=self.device)
        self.logger = get_logger("embeddings generator")

    def _generate_embeddings(self, texts):
        self.logger.info(f"Generating embeddings for {len(texts)} texts, using {self.device}")
        try:
            embeddings = self.model.encode(
                texts,
                batch_size=64,
                show_progress_bar=True,
                convert_to_numpy=True,
                normalize_embeddings=True,
                device=self.device
            )
            return embeddings
        
        except Exception as e:
            self.logger.error(f"CRITICAL: self.model.encode failed. Error: {e}", exc_info=True)
            raise

    
    def process_batch(self, reviews):
        valid_reviews = [review for review in reviews if review.get("is_valid", True)]
        texts = [review["content"] for review in valid_reviews]

        if not texts:
            self.logger.warning(f"Entire batch in {reviews[0]['firm_name']} has no text")
            for review in reviews:
                review["embedding"] = [0.0] * 384
            return reviews
        
        embeddings = self._generate_embeddings(texts)
    
        valid_review_ids = {review["review_id"]: idx for idx, review in enumerate(valid_reviews)}
        
        for review in reviews:
            if review["review_id"] in valid_review_ids:
                idx = valid_review_ids[review["review_id"]]
                embedding = embeddings[idx]
                if hasattr(embedding, 'tolist'):
                    review["embedding"] = embedding.tolist()
                elif isinstance(embedding, list):
                    review["embedding"] = embedding
                else:
                    review["embedding"] = list(embedding)
            else:
                review["embedding"] = [0.0] * 384
                self.logger.debug(f"Assigned zero embedding to invalid review {review.get('review_id')}")
        
        return reviews