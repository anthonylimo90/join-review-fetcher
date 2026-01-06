"""Scrapers for safari review sites."""
from .base import BaseScraper
from .safaribookings import SafaribookingsScraper
from .tripadvisor import TripAdvisorScraper

__all__ = ["BaseScraper", "SafaribookingsScraper", "TripAdvisorScraper"]
