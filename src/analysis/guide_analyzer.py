"""Analyzer for safari guide mentions in reviews."""
import re
import json
from typing import Optional

from textblob import TextBlob

from ..database.models import Review, GuideAnalysis


class GuideAnalyzer:
    """Analyzes reviews for safari guide mentions and sentiment."""

    # Keywords that indicate a guide mention
    GUIDE_KEYWORDS = [
        "guide",
        "driver",
        "ranger",
        "tracker",
        "spotter",
        "pilot",  # for balloon safaris
        "host",
        "naturalist",
        "instructor",
        "leader",
        "expert",
    ]

    # Common safari guide names (can be expanded)
    COMMON_GUIDE_NAMES = [
        # East African names
        "Joseph", "David", "Peter", "John", "James", "Michael", "Moses",
        "Daniel", "Samuel", "Simon", "Patrick", "Francis", "George",
        "Charles", "Edward", "William", "Paul", "Stephen", "Richard",
        "Sammy", "Wilson", "Kennedy", "Jackson", "Martin", "Victor",
        # South African names
        "Pieter", "Johan", "Willem", "Jan", "Thabo", "Sipho",
    ]

    def __init__(self):
        self._compile_patterns()

    def _compile_patterns(self):
        """Compile regex patterns for efficiency."""
        # Pattern for guide keywords
        keywords_pattern = "|".join(self.GUIDE_KEYWORDS)
        self.keyword_regex = re.compile(
            rf"\b({keywords_pattern})\b",
            re.IGNORECASE
        )

        # Pattern for "our [keyword] [Name]" or "[Name] was our [keyword]"
        self.guide_name_patterns = [
            re.compile(
                rf"\b(?:our|the|my)\s+(?:{keywords_pattern})[,\s]+([A-Z][a-z]+)\b",
                re.IGNORECASE
            ),
            re.compile(
                rf"\b([A-Z][a-z]+)\s+(?:was|is)\s+(?:our|the|my|a|an)\s+(?:{keywords_pattern})\b",
                re.IGNORECASE
            ),
            re.compile(
                rf"\b(?:{keywords_pattern})\s+(?:named|called)\s+([A-Z][a-z]+)\b",
                re.IGNORECASE
            ),
        ]

    def analyze(self, review: Review) -> GuideAnalysis:
        """Analyze a review for guide mentions."""
        text = f"{review.title} {review.text}"

        # Find keyword mentions
        keyword_matches = self.keyword_regex.findall(text)
        keywords_found = list(set(k.lower() for k in keyword_matches))

        # Extract guide names
        guide_names = self._extract_guide_names(text)

        # Check if guide is mentioned
        mentions_guide = len(keyword_matches) > 0

        # Extract sentences mentioning guides for context
        guide_context = self._extract_guide_context(text)

        # Analyze sentiment around guide mentions
        sentiment_score, sentiment_label = self._analyze_guide_sentiment(guide_context)

        return GuideAnalysis(
            review_id=review.id or 0,
            mentions_guide=mentions_guide,
            guide_names=json.dumps(guide_names),
            guide_keywords_found=json.dumps(keywords_found),
            sentiment_score=sentiment_score,
            sentiment_label=sentiment_label,
            guide_context=guide_context[:2000],  # Limit context length
        )

    def _extract_guide_names(self, text: str) -> list[str]:
        """Extract guide names from text."""
        names = []

        # Use patterns to find names
        for pattern in self.guide_name_patterns:
            matches = pattern.findall(text)
            for match in matches:
                name = match.strip()
                if name and len(name) > 1:
                    names.append(name)

        # Also check for known names in guide context
        for name in self.COMMON_GUIDE_NAMES:
            # Look for name near guide keywords
            pattern = re.compile(
                rf"\b{name}\b.{{0,30}}\b(?:guide|driver|ranger)\b|\b(?:guide|driver|ranger)\b.{{0,30}}\b{name}\b",
                re.IGNORECASE
            )
            if pattern.search(text):
                if name not in names:
                    names.append(name)

        return list(set(names))

    def _extract_guide_context(self, text: str) -> str:
        """Extract sentences that mention guides."""
        # Split into sentences
        sentences = re.split(r'[.!?]+', text)
        guide_sentences = []

        for sentence in sentences:
            if self.keyword_regex.search(sentence):
                guide_sentences.append(sentence.strip())

        return " ".join(guide_sentences)

    def _analyze_guide_sentiment(self, context: str) -> tuple[float, str]:
        """Analyze sentiment of guide-related text."""
        if not context:
            return 0.0, "neutral"

        try:
            blob = TextBlob(context)
            polarity = blob.sentiment.polarity  # -1 to 1

            if polarity > 0.1:
                label = "positive"
            elif polarity < -0.1:
                label = "negative"
            else:
                label = "neutral"

            return round(polarity, 3), label

        except Exception:
            return 0.0, "neutral"

    def get_guide_importance_indicators(self, review: Review) -> dict:
        """Get indicators of how important the guide was in the review."""
        text = f"{review.title} {review.text}".lower()
        word_count = len(text.split())

        # Count guide-related words
        guide_word_count = len(self.keyword_regex.findall(text))

        # Check for emphasis patterns
        emphasis_patterns = [
            r"\b(?:amazing|incredible|fantastic|best|excellent|wonderful|outstanding)\s+(?:guide|driver)\b",
            r"\b(?:guide|driver)\s+(?:was|is)\s+(?:amazing|incredible|fantastic|best|excellent)\b",
            r"\b(?:thanks|thank you|grateful)\b.{0,50}\b(?:guide|driver)\b",
            r"\bmade\s+(?:the|our)\s+(?:trip|safari|experience).{0,30}\b(?:guide|driver)\b",
            r"\b(?:guide|driver)\b.{0,30}\bmade\s+(?:the|our)\s+(?:trip|safari|experience)\b",
        ]

        emphasis_count = sum(
            1 for pattern in emphasis_patterns
            if re.search(pattern, text, re.IGNORECASE)
        )

        return {
            "guide_mention_density": guide_word_count / word_count if word_count > 0 else 0,
            "guide_word_count": guide_word_count,
            "total_word_count": word_count,
            "emphasis_count": emphasis_count,
            "high_importance": emphasis_count >= 2 or (guide_word_count >= 3 and review.rating >= 4),
        }


def analyze_reviews(reviews: list[Review]) -> list[GuideAnalysis]:
    """Analyze a batch of reviews."""
    analyzer = GuideAnalyzer()
    return [analyzer.analyze(review) for review in reviews]
