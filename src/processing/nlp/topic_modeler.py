from bertopic import BERTopic
from sklearn.feature_extraction.text import CountVectorizer
from sentence_transformers import SentenceTransformer
import torch 
import numpy as np
from src.utils.logger import get_logger


class TopicModeler:
    def __init__(self):
        device = "cuda" if torch.cuda.is_available() else "cpu"
        embedding_model = SentenceTransformer("all-MiniLM-L12-v2", device=device)

        self.model = self.model = BERTopic(
            language="english",
            verbose=True,
            n_gram_range=(1, 3), 
            min_topic_size=10, 
            nr_topics="auto",
            embedding_model=embedding_model,
            low_memory=False, 
            calculate_probabilities=False 
        )
        self.stop_words = [         
            # Redundant modifiers
            "really", "very", "quite", "just", "like", "also", "even",
            
            # Generic verbs (often noise)
            "use", "get", "want", "need", "make", "know",
            
            # Common but low-value nouns
            "thing", "stuff", "part", "kind"
        ]
        self.vectorizer_model = CountVectorizer(
            stop_words=self.stop_words,
            ngram_range=(1, 3),
            min_df=2
        )
        self.logger = get_logger("topic modeler")
    
    def fit_transform(self, reviews):
        texts = [rev["content"] for rev in reviews if rev.get("is_valid", True)]
        review_ids = [rev["review_id"] for rev in reviews if rev.get('is_valid', True)]
        
        if len(texts) < 20: 
            self.logger.warning(f"Too few reviews ({len(texts)}) for topic modeling")
            return {}
        self.logger.info(f"Fitting topic model on {len(texts)} reviews")

        # 1. fit model on text
        topics, probs = self.model.fit_transform(texts)

        # 2, extract topic info from text
        topic_info = self.model.get_topic_info()

        # 3. create topic assignments 
        topic_assignments = {}
        for _, (review_id, topic, prob) in enumerate(zip(review_ids, topics, probs)):
            if isinstance(prob, (float, np.float64)):
                topic_dict = {"0": float(prob)} if prob > 0.01 else {}
            else:
                topic_dict = {
                    str(i): float(p) for i, p in enumerate(prob) if p > 0.01
                }

            topic_assignments[review_id] = {
                "primary_topic_id": int(topic),
                "topic_distribution": topic_dict
            }
        
        # 4. Store topics description
        topics_data = []
        for _, row in topic_info.iterrows():
            if row["Topic"] != -1: 
                rep_docs = self.model.get_representative_docs(row['Topic'])
                
                topics_data.append({
                    "topic_id": int(row["Topic"]),
                    "topic_name": row["Name"],
                    "keywords": row["Representation"][:10],  # Top 10 keywords
                    "representative_docs": rep_docs[:3] if rep_docs else [],
                    "count": int(row["Count"])
                })
        
        return {
            "assignments": topic_assignments,
            "topics": topics_data 
        }
    
    def update_reviews_with_topics(self, reviews, topics_data):
        assignments = topics_data.get("assignments", {})

        for review in reviews:
            review_id = review.get("review_id")
            if review_id in assignments:
                review["primary_topic_id"] = assignments[review_id]["primary_topic_id"]
                review["topic_distribution"] = assignments[review_id]["topic_distribution"]
            else:
                review["primary_topic_id"] = -1
                review["topic_distribution"] = {}
        
        return reviews
