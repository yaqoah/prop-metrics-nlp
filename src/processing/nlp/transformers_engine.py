from transformers import pipeline 
from tqdm import tqdm 
from src.utils.logger import get_logger


class TransformersEngine:
    def __init__(self):
        self.sentiment_analyzer = pipeline(
            "sentiment-analysis", 
            model="ProsusAI/finbert",
            device= -1
        )
        self.emotion_analyzer = pipeline(
            "text_classification",
            model = "j-hartmann/emotion-english-distilroberta-base", 
            device= -1,
            return_all_scores=True
        )
        self.summarizer = pipeline(
            "summarization",
            model = "facebook/bart-large-cnn",
            device = -1,
            max_length=130,
            min_length=30,
            do_sample=False
        )
        self.logger = get_logger("transformers engine")

    def _process_sentiment(self, text):
        try:
            results = self.sentiment_analyzer(text[:512])[0]

            label_map = {
                "positive": "positive",
                "negative": "negative",
                "neutral": "neutral"
            }
            sentiment_label = label_map.get(results["label"].lower(), "neutral")
            sentiment_score = results["score"] if sentiment_label != "negative" else -results["score"]
            
            return {
                "sentiment_label": sentiment_label,
                "sentiment_score": sentiment_score
            }
        
        except Exception as e:
            self.logger.error(f"Error [{e}]: Sentiment analysis failed")
            return {
                "sentiment_label": "neutral",
                "sentiment_score": 0.0
            }

    def _process_emotions(self, text):
        try:
            results = self.emotion_analyzer(text[:512])[0]
            
            # Convert to dictionary with emotion scores
            emotion_scores = {
                item["label"]: round(item['score'], 4) for item in results
            }
            
            # Find dominant emotion
            dominant_emotion = max(results, key=lambda x: x["score"])["label"]
            emotion_scores["dominant"] = dominant_emotion
            
            return emotion_scores
        
        except Exception as e:
            self.logger.error(f"Error [{e}]: emotions analysis failed")
            return {}


    def _generate_summary(self, text):
        try:
            if len(text.split()) < 50:
                return text
            
            summary = self.summarizer(text[:1024])[0]["summary_text"]
            
            return summary.replace(" .", ".").strip()
        
        except Exception as e:
            self.logger.error(f"Error [{e}]: summary generation failed")
            sentences = text.split(".")[:2]
            return ". ".join(sentences) + "."
    
    def process_batch(self, reviews):
        self.logger.info(f"Processing {len(reviews)} reviews with transformers")

        for review in tqdm(review, desc="NLP Processing"):
            if not review.get("is_valid", True):
                self.logger.info("An invalid review did not go through NLP processing")
                continue

            text = review["content"]

            # review sentiment
            review["sentiment"] = self._process_sentiment(text)
            
            # review emotion
            review["emotion_scores"] = self._process_emotions(text)
            
            # review suumary
            review["summary"] = self._generate_summary(text)
        
        self.logger.info(f"Finished processing {len(reviews)} reviews with transformers")

        return reviews