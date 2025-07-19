import re
import langdetect
from src.utils.logger import get_logger


class DataValidator:
    def __init__(self):
        self.min_length = 10
        self.max_length = 1000
        self.spam_patterns = [
            r"(click here|buy now|limited offer)",
            r"(?i)\b(https?://|www\.|bit\.ly|tinyurl\.com)[^\s]+",  # URLs in reviews
            r"(?i)\b(?:\+?\d{1,3}[-. (]?)?\d{3}[-. )]?\d{3}[-. ]?\d{4}\b",  # Phone numbers
            r"(?i)\b[A-Z0-9._%+-]+@(?:gmail|yahoo|hotmail|tempmail|mailinator)\.(?:com|net)\b"  # Emails
        ]
        self.processed_hashes = set() 
        self.logger = get_logger("data validator")

    def validate_review(self, review):
        is_valid, validation_flags = True, {}

        # 1. no review content
        text = review.get("content")
        if not text:
            is_valid, validation_flags = False, {"missing_text": True}
            self.logger.warning("Review is empty")

            return is_valid, validation_flags

        # 2. valid review content length
        if len(text) < self.min_length:
            validation_flags["too short"] = True
            is_valid = False
            self.logger.warning(f"Review's length is {len(text)} - too short")
        elif len(text) > self.max_length:
            validation_flags["too long"] = True
            is_valid = False
            self.logger.warning(f"Review's length is {len(text)} - too long")
        
        # 3. language detection
        try:
            detected_lang = langdetect.detect(text)
            review["language"] = detected_lang
            if detected_lang != "en":
                validation_flags["non-english"] = True
                self.logger.warning(f"Review is not in english, it is in {detected_lang}.")
        except Exception as e:
            validation_flags["language_detection_failed"] = True
            self.logger.error(f"Error [{e}]: Failed to detect langauge of review")


        # 4. spam detection 
        for pattern in self.spam_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                validation_flags["spam"] = True
                is_valid = False
                self.logger.warning("Review is a potential spam")
                break
        
        # 5. duplicate detection 
        text_hash = hash(text[:100])
        if text_hash in self.processed_hashes:
            validation_flags["duplicate"] = True
            self.logger.warning("Review is a duplicate")
        else:
            self.processed_hashes.add(text_hash)
        
        # Finally, clean the review
        review["content"] = self.clean_text(text)

        return is_valid, validation_flags

    
    def clean_text(self, text):
        text = ' '.join(text.split()) # spaces
        text = ''.join(char for char in text if ord(char) >= 32 or char == '\n') # 
        text = re.sub(r'\n{3,}', '\n\n', text)

        return text.strip()
    
    def validate_batch(self, reviews):
        validated_reviews = []
        for review in reviews:
            is_valid, validation_flags = self.validate_review(review)
            review["is_valid"] = is_valid
            review["validation_flags"] = self.validate_flags

            validated_reviews.append(review)
        
        valid_count = sum(1 for r in validated_reviews if r["is_valid"])
        self.logger.info(f"Validated {len(reviews)} reviews: {valid_count} valid, "
                   f"{len(reviews) - valid_count} flagged")
        
        return validated_reviews
    

        