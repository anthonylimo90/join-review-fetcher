"""Analyzer for purchasing decision factors in safari reviews."""
import re
import json
from typing import Optional

from textblob import TextBlob

from ..database.models import Review, DecisionFactor


class DecisionFactorAnalyzer:
    """Analyzes reviews for purchasing decision factors."""

    # Decision factor categories and their keywords
    FACTORS = {
        "price_value": {
            "keywords": [
                "price", "cost", "expensive", "cheap", "affordable", "value",
                "worth", "money", "budget", "deal", "overpriced", "reasonable",
                "pay", "paid", "fee", "rate", "pricing",
            ],
            "phrases": [
                "value for money", "worth every", "good value", "great value",
                "bang for buck", "well worth", "not worth",
            ],
        },
        "guide_quality": {
            "keywords": [
                "guide", "driver", "ranger", "knowledgeable", "knowledge",
                "expert", "experienced", "professional", "friendly",
                "helpful", "informative", "passionate",
            ],
            "phrases": [
                "our guide", "the guide", "guide was", "driver was",
                "knew so much", "excellent guide", "amazing guide",
            ],
        },
        "wildlife_sightings": {
            "keywords": [
                "lion", "elephant", "leopard", "cheetah", "rhino", "buffalo",
                "giraffe", "zebra", "hippo", "crocodile", "wildebeest",
                "big five", "big 5", "animals", "wildlife", "sightings",
                "spotted", "saw", "seen", "migration", "birds",
            ],
            "phrases": [
                "saw the big", "spotted a", "amazing sightings",
                "close to", "up close", "in the wild",
            ],
        },
        "vehicle_equipment": {
            "keywords": [
                "vehicle", "car", "jeep", "land cruiser", "4x4", "truck",
                "comfortable", "spacious", "roof", "pop-top", "binoculars",
                "camera", "charging", "wifi", "seats",
            ],
            "phrases": [
                "open roof", "pop up roof", "comfortable vehicle",
                "plenty of room", "charging ports",
            ],
        },
        "accommodation": {
            "keywords": [
                "lodge", "camp", "tent", "hotel", "room", "accommodation",
                "stay", "stayed", "bed", "breakfast", "dinner", "food",
                "meal", "chef", "pool", "luxury", "glamping",
            ],
            "phrases": [
                "stayed at", "beautiful lodge", "amazing food",
                "tented camp", "luxury tent",
            ],
        },
        "safety_security": {
            "keywords": [
                "safe", "safety", "secure", "security", "comfortable",
                "worry", "trust", "trusted", "reliable", "careful",
                "distance", "rules", "protocol",
            ],
            "phrases": [
                "felt safe", "very safe", "no worries", "in good hands",
                "safe distance", "safety protocols",
            ],
        },
        "communication_booking": {
            "keywords": [
                "booking", "book", "booked", "email", "response", "responsive",
                "communication", "contact", "replied", "answered", "organized",
                "arranged", "planning", "itinerary", "flexible",
            ],
            "phrases": [
                "easy to book", "quick response", "well organized",
                "helped plan", "customized itinerary",
            ],
        },
        "group_size": {
            "keywords": [
                "private", "group", "people", "crowded", "small group",
                "personal", "exclusive", "shared", "other tourists",
            ],
            "phrases": [
                "private safari", "just us", "small group", "not crowded",
                "personal attention", "exclusive use",
            ],
        },
        "location_itinerary": {
            "keywords": [
                "park", "reserve", "masai mara", "serengeti", "kruger",
                "ngorongoro", "amboseli", "tarangire", "location", "locations",
                "route", "places", "destination", "destinations",
            ],
            "phrases": [
                "different parks", "various locations", "best spots",
                "off the beaten", "less touristy",
            ],
        },
    }

    def __init__(self):
        self._compile_patterns()

    def _compile_patterns(self):
        """Compile regex patterns for efficiency."""
        self.factor_patterns = {}

        for factor, config in self.FACTORS.items():
            keywords = config["keywords"]
            phrases = config.get("phrases", [])

            # Combine keywords and phrases
            all_terms = keywords + phrases
            pattern = "|".join(re.escape(term) for term in all_terms)
            self.factor_patterns[factor] = re.compile(
                rf"\b({pattern})\b",
                re.IGNORECASE
            )

    def analyze(self, review: Review) -> list[DecisionFactor]:
        """Analyze a review for decision factors."""
        text = f"{review.title} {review.text}"
        factors = []

        for factor_type, pattern in self.factor_patterns.items():
            matches = pattern.findall(text)

            if matches:
                # Extract sentences containing the factor
                mentions = self._extract_factor_context(text, pattern)

                # Analyze sentiment for this factor
                sentiment_score = self._analyze_factor_sentiment(mentions)

                # Calculate importance score based on frequency and emphasis
                importance_score = self._calculate_importance(
                    text, matches, review.rating
                )

                factors.append(DecisionFactor(
                    review_id=review.id or 0,
                    factor_type=factor_type,
                    mentions=json.dumps(mentions[:10]),  # Limit to 10 mentions
                    sentiment_score=sentiment_score,
                    importance_score=importance_score,
                ))

        return factors

    def _extract_factor_context(self, text: str, pattern: re.Pattern) -> list[str]:
        """Extract sentences containing the factor."""
        sentences = re.split(r'[.!?]+', text)
        relevant = []

        for sentence in sentences:
            if pattern.search(sentence):
                cleaned = sentence.strip()
                if cleaned and len(cleaned) > 10:
                    relevant.append(cleaned)

        return relevant

    def _analyze_factor_sentiment(self, mentions: list[str]) -> float:
        """Analyze sentiment of factor mentions."""
        if not mentions:
            return 0.0

        try:
            combined_text = " ".join(mentions)
            blob = TextBlob(combined_text)
            return round(blob.sentiment.polarity, 3)
        except Exception:
            return 0.0

    def _calculate_importance(
        self, text: str, matches: list[str], rating: float
    ) -> float:
        """Calculate importance score for a factor."""
        word_count = len(text.split())

        # Frequency score (0-1)
        frequency = len(matches) / word_count if word_count > 0 else 0
        frequency_score = min(frequency * 50, 1.0)  # Cap at 1.0

        # Rating correlation (0-1)
        rating_score = (rating / 5.0) if rating else 0.5

        # Emphasis detection
        emphasis_words = [
            "amazing", "incredible", "fantastic", "excellent", "best",
            "worst", "terrible", "awful", "disappointing", "outstanding",
        ]
        text_lower = text.lower()
        emphasis_count = sum(1 for word in emphasis_words if word in text_lower)
        emphasis_score = min(emphasis_count * 0.2, 0.5)

        # Combine scores
        importance = (frequency_score * 0.4) + (rating_score * 0.3) + (emphasis_score * 0.3)
        return round(importance, 3)

    def get_factor_summary(self, factors: list[DecisionFactor]) -> dict:
        """Get summary statistics for decision factors."""
        summary = {}

        for factor in factors:
            if factor.factor_type not in summary:
                summary[factor.factor_type] = {
                    "count": 0,
                    "total_importance": 0,
                    "total_sentiment": 0,
                }

            summary[factor.factor_type]["count"] += 1
            summary[factor.factor_type]["total_importance"] += factor.importance_score
            summary[factor.factor_type]["total_sentiment"] += factor.sentiment_score

        # Calculate averages
        for factor_type, stats in summary.items():
            if stats["count"] > 0:
                stats["avg_importance"] = stats["total_importance"] / stats["count"]
                stats["avg_sentiment"] = stats["total_sentiment"] / stats["count"]
            else:
                stats["avg_importance"] = 0
                stats["avg_sentiment"] = 0

        return summary


def analyze_reviews(reviews: list[Review]) -> list[DecisionFactor]:
    """Analyze a batch of reviews for decision factors."""
    analyzer = DecisionFactorAnalyzer()
    all_factors = []

    for review in reviews:
        factors = analyzer.analyze(review)
        all_factors.extend(factors)

    return all_factors
