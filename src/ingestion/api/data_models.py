from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional, List, Dict

@dataclass
class Review:
    review_id: str
    author_name: str
    rating: int
    title: str
    content: str
    date_posted: datetime
    verified: bool = False
    date_of_experience: Optional[datetime] = None
    reply_content: Optional[str] = None
    reply_date: Optional[datetime] = None
    author_reviews_count: Optional[int] = None
    author_location: Optional[str] = None

    def validate(self):
        errors = []
        if not self.review_id:
            errors.append("Review is missing an ID")
        if not self.author_name:
            errors.append("Review is missing author name") 
        if not 1 <= self.rating <= 5:
            print(f"Parsed review: rating={self.rating}")
            errors.append(f"Review's [{self.rating}] rating is invalid")
        if not self.content or len(self.content.strip()) < 10:
            print(f"Parsed review: content='{self.content}'")
            errors.append("Review content is too short")
        if self.date_posted > datetime.now():
            errors.append("Review posted in future date")
            
        return errors
    
    def to_dict(self):
        data = asdict(self)
        for key, value in data.items():
            if isinstance(value, datetime): # format date
                data[key] = value.isoformat()

        return data
    
    @classmethod
    def from_dict(cls, data: dict):
        for key in ["date_posted", "date_of_experience", "reply_date"]:
            if key in data and data[key]:
                data[key] = datetime.fromisoformat(data[key])
        
        return cls(**data)

@dataclass
class Firm:
    name: str
    trustpilot_url: str
    rating: float
    total_reviews: int
    rating_distribution: Dict[int, int]
    claimed: bool
    website: Optional[str] = None
    category: Optional[str] = None
    description: Optional[str] = None

    def validate(self):
        errors = []
        if not self.name:
            errors.append("Firm missing name")    
        if not 0 <= self.rating <= 5:
            errors.append(f"Firm's [{self.rating}] rating is invalid")
        if self.total_reviews < 0:
            errors.append(f"Firm's [{self.total_reviews}] total reviews is invalid")
        if sum(self.rating_distribution.values()) > self.total_reviews:
            errors.append("Firm's rating distribution sum exceeds total reviews")
            
        return errors
    
    def to_dict(self):
        return asdict(self)

@dataclass
class Session:
    firm_name: str
    trustpilot_url: str
    firm_data: Firm
    reviews: List[Review]
    scrape_date: datetime
    scraper_used: str
    total_pages: int
    success: bool
    errors: List[str] = None

    def to_dict(self):
        return {
            "firm_name": self.firm_name,
            "trustpilot_url": self.trustpilot_url,
            "firm_data": self.firm_data.to_dict() if self.firm_data else None,
            "reviews": [r.to_dict() for r in self.reviews],
            "metadata": {
                "scrape_date": self.scrape_date.isoformat(),
                "scraper_used": self.scraper_used,
                "total_pages": self.total_pages,
                "total_reviews": len(self.reviews),
                "success": self.success,
                "errors": self.errors or []
            }
        }