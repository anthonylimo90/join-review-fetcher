"""Analyzer for demographic information from reviews."""
import re
from typing import Optional

from ..database.models import Review, Demographic


class DemographicsAnalyzer:
    """Extracts demographic information from reviews and reviewer data."""

    # Region mapping based on countries
    REGION_MAPPING = {
        # North America
        "NA": [
            "united states", "usa", "us", "america", "canada", "mexico",
            "california", "texas", "florida", "new york", "illinois",
            "arizona", "colorado", "washington", "oregon", "nevada",
            "massachusetts", "pennsylvania", "ohio", "georgia", "michigan",
            "toronto", "vancouver", "montreal", "ontario", "quebec", "alberta",
        ],
        # United Kingdom
        "UK": [
            "united kingdom", "uk", "england", "scotland", "wales",
            "northern ireland", "britain", "british", "london", "manchester",
            "birmingham", "liverpool", "edinburgh", "glasgow", "bristol",
            "leeds", "sheffield", "nottingham", "leicester", "cardiff",
        ],
        # Europe
        "EU": [
            "germany", "france", "italy", "spain", "netherlands", "belgium",
            "switzerland", "austria", "sweden", "norway", "denmark", "finland",
            "poland", "czech", "portugal", "ireland", "greece", "hungary",
            "romania", "croatia", "slovenia", "slovakia", "luxembourg",
            "german", "french", "italian", "spanish", "dutch", "belgian",
            "swiss", "austrian", "swedish", "norwegian", "danish", "finnish",
            "berlin", "paris", "rome", "madrid", "amsterdam", "brussels",
            "vienna", "stockholm", "oslo", "copenhagen", "helsinki",
            "munich", "hamburg", "frankfurt", "barcelona", "milan",
        ],
    }

    # Travel composition indicators
    COMPOSITION_PATTERNS = {
        "solo": [
            r"\bsolo\b", r"\balone\b", r"\bby myself\b", r"\bon my own\b",
            r"\bsingle traveler\b",
        ],
        "couple": [
            r"\bcouple\b", r"\bhoneymoon\b", r"\bmy (?:wife|husband|partner|spouse)\b",
            r"\bwith my (?:wife|husband|partner|girlfriend|boyfriend)\b",
            r"\bjust (?:the )?two of us\b", r"\bromantic\b", r"\banniversary\b",
        ],
        "family": [
            r"\bfamily\b", r"\b(?:our |my )?kids?\b", r"\bchildren\b",
            r"\bwith (?:our |my )?(?:son|daughter)\b",
            r"\bfamily of \d+\b", r"\bfamily vacation\b", r"\bfamily trip\b",
        ],
        "friends": [
            r"\bfriends?\b", r"\bgroup of friends\b", r"\bwith friends\b",
            r"\bfriendship\b",
        ],
        "group": [
            r"\bgroup\b", r"\btour group\b", r"\borganized tour\b",
            r"\b\d+ (?:of us|people)\b",
        ],
    }

    # Age/life stage indicators
    AGE_PATTERNS = {
        "retired": [
            r"\bretired\b", r"\bretirement\b", r"\bgolden years\b",
            r"\bsenior\b", r"\b(?:over |above )?(?:60|65|70)\b",
        ],
        "honeymoon": [
            r"\bhoneymoon\b", r"\bjust married\b", r"\bnewlywed\b",
        ],
        "family_with_kids": [
            r"\bkids?\b", r"\bchildren\b", r"\byoung (?:son|daughter)\b",
            r"\btoddler\b", r"\bteenager\b", r"\bteen\b",
        ],
        "young_professional": [
            r"\b(?:20s|thirties|30s)\b", r"\bgraduation\b", r"\bfirst time\b",
        ],
        "middle_aged": [
            r"\b(?:40s|50s|forties|fifties)\b", r"\bmidlife\b",
            r"\bbucket list\b",
        ],
    }

    # Experience level patterns
    EXPERIENCE_PATTERNS = {
        "first_safari": [
            r"\bfirst (?:time |ever )?safari\b", r"\bfirst safari\b",
            r"\bnever been (?:on )?(?:a )?safari\b", r"\bfirst time (?:in )?africa\b",
            r"\bdream (?:safari|trip)\b", r"\bbucket list\b",
        ],
        "repeat": [
            r"\b(?:second|third|\d+(?:st|nd|rd|th)) safari\b",
            r"\breturn(?:ed|ing)? to\b", r"\bback to\b",
            r"\bvisited (?:\w+ )?before\b", r"\brepeat\b",
            r"\bmany safaris?\b", r"\bseveral safaris?\b",
        ],
    }

    def __init__(self):
        self._compile_patterns()

    def _compile_patterns(self):
        """Compile regex patterns."""
        # Compile composition patterns
        self.composition_compiled = {}
        for comp_type, patterns in self.COMPOSITION_PATTERNS.items():
            combined = "|".join(patterns)
            self.composition_compiled[comp_type] = re.compile(
                combined, re.IGNORECASE
            )

        # Compile age patterns
        self.age_compiled = {}
        for age_type, patterns in self.AGE_PATTERNS.items():
            combined = "|".join(patterns)
            self.age_compiled[age_type] = re.compile(combined, re.IGNORECASE)

        # Compile experience patterns
        self.experience_compiled = {}
        for exp_type, patterns in self.EXPERIENCE_PATTERNS.items():
            combined = "|".join(patterns)
            self.experience_compiled[exp_type] = re.compile(
                combined, re.IGNORECASE
            )

        # Party size pattern
        self.party_size_pattern = re.compile(
            r"\b(?:group of |party of |(\d+) (?:of us|people)|\bwe were (\d+)\b)",
            re.IGNORECASE
        )

    def analyze(self, review: Review) -> Demographic:
        """Analyze a review for demographic information."""
        text = f"{review.title} {review.text}"

        # Determine region from reviewer location
        country, region = self._classify_region(review.reviewer_location)

        # If no location, try to infer from text
        if not region:
            country, region = self._infer_region_from_text(text)

        # Determine travel composition
        travel_composition = self._detect_composition(text, review.trip_type)

        # Estimate party size
        party_size = self._extract_party_size(text)

        # Determine experience level
        experience_level = self._detect_experience(text)

        # Detect age/life stage indicators
        age_indicator = self._detect_age_indicator(text)

        return Demographic(
            review_id=review.id or 0,
            country=country,
            region=region,
            travel_composition=travel_composition,
            party_size=party_size,
            experience_level=experience_level,
            age_indicator=age_indicator,
        )

    def _classify_region(self, location: str) -> tuple[str, str]:
        """Classify a location into country and region."""
        if not location:
            return "", ""

        location_lower = location.lower()

        # Check each region
        for region, indicators in self.REGION_MAPPING.items():
            for indicator in indicators:
                if indicator in location_lower:
                    return location, region

        return location, "Other"

    def _infer_region_from_text(self, text: str) -> tuple[str, str]:
        """Try to infer region from review text."""
        text_lower = text.lower()

        # Look for patterns like "we're from", "visiting from", "came from"
        from_patterns = [
            r"(?:we're |we are |i'm |i am |visiting |came |coming |traveled? )?from ([A-Za-z\s,]+)",
            r"as (?:a |an )?([A-Za-z]+) tourist",
        ]

        for pattern in from_patterns:
            match = re.search(pattern, text_lower)
            if match:
                potential_location = match.group(1).strip()
                return self._classify_region(potential_location)

        return "", ""

    def _detect_composition(self, text: str, trip_type: str) -> str:
        """Detect travel composition from text and trip type."""
        # First check explicit trip type from scraper
        if trip_type:
            trip_lower = trip_type.lower()
            if "solo" in trip_lower:
                return "solo"
            elif "couple" in trip_lower:
                return "couple"
            elif "family" in trip_lower:
                return "family"
            elif "friend" in trip_lower:
                return "friends"
            elif "group" in trip_lower:
                return "group"

        # Detect from text
        text_lower = text.lower()
        composition_scores = {}

        for comp_type, pattern in self.composition_compiled.items():
            matches = pattern.findall(text_lower)
            if matches:
                composition_scores[comp_type] = len(matches)

        if composition_scores:
            return max(composition_scores, key=composition_scores.get)

        return ""

    def _extract_party_size(self, text: str) -> Optional[int]:
        """Extract party size from text."""
        matches = self.party_size_pattern.findall(text)

        for match in matches:
            for group in match:
                if group and group.isdigit():
                    size = int(group)
                    if 1 <= size <= 50:  # Reasonable range
                        return size

        # Infer from composition
        text_lower = text.lower()
        if re.search(r"\bsolo\b|\balone\b", text_lower):
            return 1
        elif re.search(r"\bcouple\b|\btwo of us\b", text_lower):
            return 2

        return None

    def _detect_experience(self, text: str) -> str:
        """Detect safari experience level."""
        for exp_type, pattern in self.experience_compiled.items():
            if pattern.search(text):
                return exp_type

        return ""

    def _detect_age_indicator(self, text: str) -> str:
        """Detect age/life stage indicator."""
        for age_type, pattern in self.age_compiled.items():
            if pattern.search(text):
                return age_type

        return ""

    def is_target_demographic(self, demographic: Demographic) -> bool:
        """Check if demographic matches target (NA/UK/EU)."""
        return demographic.region in ["NA", "UK", "EU"]


def analyze_reviews(reviews: list[Review]) -> list[Demographic]:
    """Analyze a batch of reviews for demographics."""
    analyzer = DemographicsAnalyzer()
    return [analyzer.analyze(review) for review in reviews]
