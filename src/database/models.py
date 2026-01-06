"""Database models for safari reviews."""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
import json


@dataclass
class Review:
    """Represents a safari review from any source."""
    id: Optional[int] = None
    source: str = ""  # 'tripadvisor' or 'safaribookings'
    url: str = ""
    operator_name: str = ""
    reviewer_name: str = ""
    reviewer_location: str = ""
    reviewer_country: str = ""
    rating: float = 0.0
    title: str = ""
    text: str = ""
    travel_date: Optional[str] = None
    review_date: Optional[str] = None
    trip_type: str = ""  # solo, couple, family, friends, group
    scraped_at: str = field(default_factory=lambda: datetime.now().isoformat())

    # Reviewer profile stats
    reviewer_contributions: int = 0  # Total reviews by this reviewer
    reviewer_helpful_votes: int = 0  # Total helpful votes received by reviewer

    # Review metadata
    helpful_votes: int = 0  # Helpful votes on this specific review
    review_id_source: str = ""  # Original ID from source platform

    # Safari-specific extracted data
    age_range: str = ""  # e.g., "35-50" years
    parks_visited: str = "[]"  # JSON list of park names
    wildlife_sightings: str = "[]"  # JSON list of animals seen
    guide_names_mentioned: str = "[]"  # JSON list of guide names in text
    safari_duration_days: Optional[int] = None  # Trip length if mentioned

    # Parsing metadata (for debugging and quality tracking)
    parsing_confidence: float = 1.0  # 0-1 confidence in parse quality
    raw_text_block: str = ""  # Original unparsed text block
    parse_warnings: str = "[]"  # JSON list of parsing issues

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "source": self.source,
            "url": self.url,
            "operator_name": self.operator_name,
            "reviewer_name": self.reviewer_name,
            "reviewer_location": self.reviewer_location,
            "reviewer_country": self.reviewer_country,
            "rating": self.rating,
            "title": self.title,
            "text": self.text,
            "travel_date": self.travel_date,
            "review_date": self.review_date,
            "trip_type": self.trip_type,
            "scraped_at": self.scraped_at,
            "reviewer_contributions": self.reviewer_contributions,
            "reviewer_helpful_votes": self.reviewer_helpful_votes,
            "helpful_votes": self.helpful_votes,
            "review_id_source": self.review_id_source,
            "age_range": self.age_range,
            "parks_visited": self.parks_visited,
            "wildlife_sightings": self.wildlife_sightings,
            "guide_names_mentioned": self.guide_names_mentioned,
            "safari_duration_days": self.safari_duration_days,
            "parsing_confidence": self.parsing_confidence,
            "raw_text_block": self.raw_text_block,
            "parse_warnings": self.parse_warnings,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Review":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

    @property
    def wildlife_list(self) -> list:
        """Get wildlife sightings as a list."""
        return json.loads(self.wildlife_sightings) if self.wildlife_sightings else []

    @property
    def guide_names_list(self) -> list:
        """Get guide names as a list."""
        return json.loads(self.guide_names_mentioned) if self.guide_names_mentioned else []

    @property
    def parks_list(self) -> list:
        """Get parks visited as a list."""
        return json.loads(self.parks_visited) if self.parks_visited else []

    @property
    def warnings_list(self) -> list:
        """Get parse warnings as a list."""
        return json.loads(self.parse_warnings) if self.parse_warnings else []


@dataclass
class GuideAnalysis:
    """Analysis of guide mentions in a review."""
    id: Optional[int] = None
    review_id: int = 0
    mentions_guide: bool = False
    guide_names: str = "[]"  # JSON list of extracted names
    guide_keywords_found: str = "[]"  # JSON list of keywords
    sentiment_score: float = 0.0  # -1 to 1
    sentiment_label: str = ""  # positive, negative, neutral
    guide_context: str = ""  # extracted sentences mentioning guide

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "review_id": self.review_id,
            "mentions_guide": self.mentions_guide,
            "guide_names": self.guide_names,
            "guide_keywords_found": self.guide_keywords_found,
            "sentiment_score": self.sentiment_score,
            "sentiment_label": self.sentiment_label,
            "guide_context": self.guide_context,
        }

    @property
    def guide_names_list(self) -> list:
        return json.loads(self.guide_names)

    @property
    def keywords_list(self) -> list:
        return json.loads(self.guide_keywords_found)


@dataclass
class DecisionFactor:
    """Purchasing decision factors extracted from a review."""
    id: Optional[int] = None
    review_id: int = 0
    factor_type: str = ""  # price, safety, vehicle, guide, wildlife, etc.
    mentions: str = "[]"  # JSON list of text excerpts
    sentiment_score: float = 0.0
    importance_score: float = 0.0  # based on emphasis in text

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "review_id": self.review_id,
            "factor_type": self.factor_type,
            "mentions": self.mentions,
            "sentiment_score": self.sentiment_score,
            "importance_score": self.importance_score,
        }


@dataclass
class Demographic:
    """Demographic information extracted from a review/reviewer."""
    id: Optional[int] = None
    review_id: int = 0
    country: str = ""
    region: str = ""  # NA, UK, EU, Other
    travel_composition: str = ""  # solo, couple, family, friends, group
    party_size: Optional[int] = None
    experience_level: str = ""  # first_safari, repeat
    age_indicator: str = ""  # retired, honeymoon, family_with_kids, etc.

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "review_id": self.review_id,
            "country": self.country,
            "region": self.region,
            "travel_composition": self.travel_composition,
            "party_size": self.party_size,
            "experience_level": self.experience_level,
            "age_indicator": self.age_indicator,
        }
