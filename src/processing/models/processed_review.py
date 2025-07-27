from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import datetime

@dataclass
class ProcessedReview:
    # Original data
    firm_name: str
    review_id: str
    author_name: Optional[str]
    rating: Optional[int]
    date_posted: Optional[datetime]
    content: str
    title: Optional[str]
    
    # Validation
    is_valid: bool = True
    validation_flags: Dict = field(default_factory=dict)
    language: Optional[str] = None
    
    # NLP enrichments
    sentiment_score: Optional[float] = None
    sentiment_label: Optional[str] = None
    emotion_scores: Dict = field(default_factory=dict)
    summary: Optional[str] = None
    
    # Extractions
    entities: Dict = field(default_factory=dict)
    aspects: List[Dict] = field(default_factory=list)
    key_phrases: List[str] = field(default_factory=list)
    
    # Topic modeling
    primary_topic_id: Optional[int] = None
    topic_distribution: Dict = field(default_factory=dict)
    
    # Embeddings
    embedding: Optional[List[float]] = None
    
    # Metadata
    processed_at: Optional[datetime] = None
    processing_version: str = "1.0"


    def to_dict(self):
        return {
            "firm_name": self.firm_name,
            "review_id": self.review_id,
            "author_name": self.author_name,
            "rating": self.rating,
            "date_posted": self.review_date,
            "content": self.review_text,
            "title": self.review_title,
            "is_valid": self.is_valid,
            "validation_flags": self.validation_flags,
            "language": self.language,
            "sentiment_score": self.sentiment_score,
            "sentiment_label": self.sentiment_label,
            "emotion_scores": self.emotion_scores,
            "summary": self.summary,
            "entities": self.entities,
            "aspects": self.aspects,
            "key_phrases": self.key_phrases,
            "primary_topic_id": self.primary_topic_id,
            "topic_distribution": self.topic_distribution,
            "embedding": self.embedding,
            "processed_at": self.processed_at or datetime.now(),
            "processing_version": self.processing_version
        }