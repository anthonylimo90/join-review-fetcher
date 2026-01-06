"""Review validation and parsing error tracking."""
import json
import re
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime

from ..database.models import Review
from .country_codes import COUNTRY_CODES


@dataclass
class ParseResult:
    """Result of a parsing attempt with detailed diagnostics."""
    success: bool
    review: Optional[Review] = None
    confidence: float = 1.0
    warnings: list = field(default_factory=list)
    raw_block: str = ""
    strategy_used: str = ""


class ReviewValidator:
    """Validate extracted review data without dropping reviews."""

    MIN_TEXT_LENGTH = 10  # Reduced from 30 to capture more reviews
    MAX_TEXT_LENGTH = 50000
    VALID_RATING_RANGE = (0, 5)

    def validate(self, review: Review) -> tuple[bool, list[str]]:
        """
        Validate review and return (is_valid, warnings).

        Key principle: Always accept reviews, but track quality warnings.
        We never silently drop data - instead we flag issues for analysis.
        """
        warnings = []

        # Text length check (warn but don't reject)
        text_len = len(review.text.strip()) if review.text else 0
        if text_len < self.MIN_TEXT_LENGTH:
            warnings.append(f"short_text:{text_len}")
        elif text_len > self.MAX_TEXT_LENGTH:
            warnings.append("truncated_text")
            review.text = review.text[:self.MAX_TEXT_LENGTH]

        # Rating validation
        if review.rating is not None:
            if review.rating < self.VALID_RATING_RANGE[0] or review.rating > self.VALID_RATING_RANGE[1]:
                warnings.append(f"invalid_rating:{review.rating}")
                review.rating = max(self.VALID_RATING_RANGE[0],
                                   min(self.VALID_RATING_RANGE[1], review.rating))

        # Country code validation
        if review.reviewer_country:
            code = review.reviewer_country.upper()
            if len(code) == 2 and code not in COUNTRY_CODES:
                warnings.append(f"unknown_country:{code}")

        # Date format validation
        if review.travel_date and not self._validate_date(review.travel_date):
            warnings.append(f"invalid_travel_date:{review.travel_date}")

        if review.review_date and not self._validate_date(review.review_date):
            warnings.append(f"invalid_review_date:{review.review_date}")

        # Reviewer name validation
        if not review.reviewer_name or len(review.reviewer_name.strip()) < 2:
            warnings.append("missing_reviewer_name")

        # Empty review check (still accept but flag)
        if not review.text or not review.text.strip():
            warnings.append("empty_text")

        # URL validation
        if not review.url:
            warnings.append("missing_url")

        # Always accept the review - we track quality issues but don't drop data
        is_valid = True

        # Store warnings in the review
        if warnings:
            existing = json.loads(review.parse_warnings) if review.parse_warnings else []
            review.parse_warnings = json.dumps(existing + warnings)

        return is_valid, warnings

    def _validate_date(self, date_str: str) -> bool:
        """Validate date string format."""
        if not date_str:
            return True  # Empty is valid (just missing)

        patterns = [
            r'^(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}$',
            r'^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},?\s*\d{4}$',
            r'^\d{1,2}/\d{4}$',
            r'^\d{4}-\d{2}(-\d{2})?$',
            r'^\w+\s+\d{4}$',  # Generic "Month Year" format
        ]
        return any(re.match(p, date_str, re.IGNORECASE) for p in patterns)


class ParsingErrorTracker:
    """Track parsing errors and generate quality reports."""

    def __init__(self):
        self.errors: list[dict] = []
        self.warnings: list[dict] = []
        self.stats = {
            'total_attempted': 0,
            'successful': 0,
            'failed': 0,
            'low_confidence': 0,
            'with_warnings': 0,
        }
        self.strategy_usage: dict[str, int] = {}

    def record_attempt(self, result: ParseResult):
        """Record a parsing attempt result."""
        self.stats['total_attempted'] += 1

        # Track strategy usage
        if result.strategy_used:
            self.strategy_usage[result.strategy_used] = \
                self.strategy_usage.get(result.strategy_used, 0) + 1

        if result.success:
            self.stats['successful'] += 1

            if result.confidence < 0.7:
                self.stats['low_confidence'] += 1
                self.warnings.append({
                    'type': 'low_confidence',
                    'confidence': result.confidence,
                    'raw_block': result.raw_block[:200] if result.raw_block else '',
                    'warnings': result.warnings,
                    'strategy': result.strategy_used,
                    'timestamp': datetime.now().isoformat(),
                })

            if result.warnings:
                self.stats['with_warnings'] += 1
        else:
            self.stats['failed'] += 1
            self.errors.append({
                'raw_block': result.raw_block[:500] if result.raw_block else '',
                'warnings': result.warnings,
                'strategy': result.strategy_used,
                'timestamp': datetime.now().isoformat(),
            })

    def get_report(self) -> dict:
        """Generate a quality report."""
        total = max(self.stats['total_attempted'], 1)
        return {
            'stats': self.stats,
            'success_rate': self.stats['successful'] / total,
            'failure_rate': self.stats['failed'] / total,
            'low_confidence_rate': self.stats['low_confidence'] / total,
            'warning_rate': self.stats['with_warnings'] / total,
            'strategy_usage': self.strategy_usage,
            'error_samples': self.errors[:10],
            'warning_samples': self.warnings[:10],
        }

    def get_summary(self) -> str:
        """Get a human-readable summary."""
        report = self.get_report()
        return (
            f"Parsing Report:\n"
            f"  Total attempted: {report['stats']['total_attempted']}\n"
            f"  Success rate: {report['success_rate']:.1%}\n"
            f"  Failed: {report['stats']['failed']}\n"
            f"  Low confidence: {report['stats']['low_confidence']}\n"
            f"  With warnings: {report['stats']['with_warnings']}\n"
            f"  Strategy usage: {report['strategy_usage']}"
        )

    def reset(self):
        """Reset all tracking data."""
        self.errors = []
        self.warnings = []
        self.stats = {
            'total_attempted': 0,
            'successful': 0,
            'failed': 0,
            'low_confidence': 0,
            'with_warnings': 0,
        }
        self.strategy_usage = {}
