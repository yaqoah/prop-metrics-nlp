import spacy
from spacy.matcher import Matcher
from collections import Counter
from src.utils.logger import get_logger

class SpacyExtractor:
    def __init__(self):
        self.nlp = spacy.load("en_core_web_sm")
        self.matcher = Matcher(self.nlp.vocab)
        self.aspect_keywords = {
            "funding": ["funding", "challenge fee", "evaluation fee", 
                                "profit split", "subscription"],
            "rules": ["rules", "restrictions", "drawdown", "daily loss", 
                            "consistency rule", "trading days"],
            "platform": ["platform", "software", 'mt4', 'mt5', "tradingview", "topstepx",
                                "ctrader", "match trader", "dxtrade", "tradelocker"],
            "spreads": ["spread", "spreads", "pip", "pips", "commission"],
            "features": ["charting", "indicators", "EA"],
            "execution": ["execution", "slippage", "speed", "latency", "fill"],
            "support": ["support", "customer service", "help", "response", "team"],
            "withdrawal": ["withdrawal", "withdraw", "payout", "payment", "funds"],
            "verification": ["verification", "kyc", "documents", "identity", "verify"],
            "leverage": ["leverage", "margin", "lot", "position size"],
            "reliability": ["reliable", "trust", "scam", "legit", "honest"],
            "stability": ["crash", "bug", "glich", "lag", "freeze", "downtime"]
        }
        self._add_trading_patterns()
        self.logger = get_logger("spacy extractor")

    def _add_trading_patterns(self):
        instrument_pattern = [
            {"TEXT": {"REGEX": "^(EUR|USD|GBP|JPY|AUD|CAD|CHF|NZD|CNY|SGD|HKD|MXN|ZAR|BTC|ETH)$"}},
            {"TEXT": {"REGEX": "^[/]?$"}, "OP": "?"},
            {"TEXT": {"REGEX": "^(EUR|USD|GBP|JPY|AUD|CAD|CHF|NZD|CNY|SGD|HKD|MXN|ZAR|BTC|ETH)$"}}
        ]
        self.matcher.add("INSTRUMENT", [instrument_pattern])

        money_pattern = [
            {"TEXT": {"REGEX": r"^(\d+|\d{1,3}(,\d{3})*)(\.\d+)?$"}},
            {"TEXT": {"IN": ["$", "USD", "EUR", "GBP", "JPY", "BTC", "ETH", "£", "€", "¥"]}, "OP": "?"},
            {"LOWER": {"IN": ["million", "billion", "trillion"]}, "OP": "?"}
        ]
        self.matcher.add("MONEY_AMOUNT", [money_pattern])

        signal_pattern = [
            {"LOWER": {"IN": ["buy", "sell", "long", "short", "bullish", "bearish"]}}
        ]
        self.matcher.add("TRADING_SIGNAL", [signal_pattern])

        order_pattern = [
            {"LOWER": {"IN": ["limit", "stop", "market", "entry", "take", "profit", "loss"]}}
        ]
        self.matcher.add("ORDER_TYPE", [order_pattern])


        self.matcher.add("PRICE_MOVEMENT", [[{"LIKE_NUM": True}, {"LOWER": {"IN": ["pips", "points", "ticks"]}}]])
        self.matcher.add("TIMEFRAME", [[{"TEXT": {"REGEX": r"^\d+[mhdwM]$"}}]])

    def _extract_entities(self, text):
        doc = self.nlp(text)

        entities = {
            "standard": [],
            "trading": [],
            "money": [],
            "price": [], 
            "order": [],
            "timeframes": []
        }

        # 1. standar ner
        for ent in doc.ents:
            if ent.label_ in ["MONEY", "DATE", "ORG", "PRODUCT"]:
                entities["standard"].append({
                    "text": ent.text,
                    "type": ent.label_
                })
        
        # 2. custom pattern matcher
        matches = self.matcher(doc)
        for match_id, start, end in matches:
            span = doc[start:end]
            match_type = self.nlp.vocab.strings[match_id]

            if match_type == "INSTRUMENT":
                entities["trading"].append({
                    "text": span.text,
                    "type": "instrument"
                })
            elif match_type == "TRADING_SIGNAL":
                entities["trading"].append({
                    "text": span.text,
                    "type": "trading_signal"
                })
            elif match_type == "MONEY_AMOUNT":
                entities["money"].append({
                    "text": span.text,
                    "context": doc[max(0, start-5):min(len(doc), end+5)].text
                })
            elif match_type == "PRICE_MOVEMENT":
                entities["price"].append({
                    "text": span.text,
                    "type": "price_movement"
                })
            elif match_type == "ORDER_TYPE":
                entities["order"].append({
                    "text": span.text,
                    "type": "order_type"
                })
            elif match_type == "TIMEFRAME":
                entities["timeframes"].append({
                    "text": span.text,
                    "type": "timeframe"
                })
        
        return entities

    def _extract_aspects(self, text):
        doc = self.nlp(text.lower())
        found_aspects = []

        for aspect, keywords in self.aspect_keywords.items():
            aspect_mentions = []      
            for keyword in keywords:
                for token in doc:
                    if keyword in token.text:
                        start = max(0, token.i - 5)
                        end = min(len(doc), token.i + 6)
                        context = doc[start:end].text
                        sentiment = self._get_context_sentiment(doc[start:end])
                        aspect_mentions.append({
                            "keyword": keyword,
                            "context": context,
                            "sentiment": sentiment
                        })

            if aspect_mentions:
                found_aspects.append({
                    "aspect": aspect,
                    "mentions": len(aspect_mentions),
                    "details": aspect_mentions[:3]  # Keep top 3 mentions
                })
        
        return found_aspects

    def _get_contextual_sentiment(self, span):
        positive_words = {
            # Platform & Execution
            "smooth", "reliable", "stable", "instant", "seamless", "accurate", 
            "tight", "fast", "low latency", "no slippage", "zero requotes",
            
            # Support & Funding
            "responsive", "helpful", "transparent", "fair", "generous", "flexible",
            "quick payout", "hassle-free", 

            # General Praise
            "excellent", "outstanding", "top-notch", "recommend", "satisfied",
            "good", "great", "excellent", "fast", "best", "love"
        }

        negative_words = {
            # Platform & Execution
            "slow", "laggy", "buggy", "crash", "freeze", "unstable", "janky",
            "wide", "high","slippage", "requotes", "rejected", "partial fills",

            # Support & Funding
            "unresponsive", "rude", "delayed", "hidden fees", "rigged", "strict",
            "slow payout", "withdrawal issues", "verification hell",

            # General Complaints
            "scam", "fraud", "dishonest", "worst", "avoid", "frustrating"
        }

        text = span.text.lower()
        pos_count = sum(1 for word in positive_words if word in text)
        neg_count = sum(1 for word in negative_words if word in text)
        
        if pos_count > neg_count:
            return "positive"
        elif neg_count > pos_count:
            return "negative"
        
        return "neutral"

    def _extract_key_phrases(self, text):
        doc = self.nlp(text)
        key_phrases = []
        
        # 1.  noun chunks
        for chunk in doc.noun_chunks:
            if 2 <= len(chunk) <= 5:
                key_phrases.append(chunk.text.lower())
        
        # 2. verb phrases with object
        for token in doc:
            if token.pos_ == "VERB" and token.dep_ == "ROOT":
                phrase_tokens = [token]
                for child in token.children:
                    if child.dep_ in ["dobj", "pobj", "attr"]:
                        phrase_tokens.append(child)
                if len(phrase_tokens) > 1:
                    phrase = ' '.join(t.text for t in sorted(phrase_tokens, key=lambda x: x.i))
                    key_phrases.append(phrase.lower())
        
        phrase_counts = Counter(key_phrases)
        return [phrase for phrase, _ in phrase_counts.most_common(10)]
        
    def process_batch(self, reviews):
        self.logger.info(f"Extracting entities and aspects from {len(reviews)} reviews")
        
        for review in reviews:
            if not review.get("is_valid", True):
                self.logger.info("An invalid review did not go through SpaCy extraction")
                continue

            text = review["content"]

            # extract entities
            review["entities"] = self._extract_entities(text)
            
            # extract aspects
            review["aspects"] = self._extract_aspects(text)
            
            # extract key phrases
            review["key_phrases"] = self._extract_key_phrases(text)

        return reviews