import numpy as np
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
from src.utils.logger import get_logger


class EmbeddingsGenerator:
    def __init__(self):
        self.model = SentenceTransformer("all-MiniLM-L6-v2")
        self.logger = get_logger("embeddings generator")

    def _generate_embeddings(self, texts):
        self.logger.info(f"Generating embeddings for {len(texts)} texts")
        embeddings = self.model.encode(
            texts,
            batch_size=32,
            show_progress_bar=True,
            convert_to_numpy=True,
            normalize_embeddings=True  
        )

        return embeddings

    
    def process_batch(self, reviews):
        valid__reviews = [review for review in reviews if review.get("is_valid", True)]
        texts = [review["content"] for review in valid__reviews]

        if not texts:
            self.logger.warning(f"Entire batch in {reviews[0]['firm_name']} has no text")
            return reviews
        
        embeddings_idx, embeddings = 0, self._generate_embeddings(texts)
        for review in valid__reviews:
            review["embedding"] = embeddings[embeddings_idx].tolist()
            embeddings_idx += 1
        else: 
            review["embedding"] = [0.0] * 384
        
        return reviews