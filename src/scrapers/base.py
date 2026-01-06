"""Base scraper class with common functionality."""
import asyncio
import random
import json
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional
from datetime import datetime

from playwright.async_api import async_playwright, Page, Browser


class ScraperState:
    """Manages scraper state for pause/resume functionality."""

    def __init__(self, state_file: str = "data/scraper_state.json"):
        self.state_file = Path(state_file)
        self.state_file.parent.mkdir(parents=True, exist_ok=True)

    def save(self, scraper_name: str, data: dict):
        """Save scraper state."""
        state = self.load_all()
        state[scraper_name] = {
            **data,
            "updated_at": datetime.now().isoformat()
        }
        with open(self.state_file, "w") as f:
            json.dump(state, f, indent=2)

    def load(self, scraper_name: str) -> Optional[dict]:
        """Load scraper state."""
        state = self.load_all()
        return state.get(scraper_name)

    def load_all(self) -> dict:
        """Load all scraper states."""
        if self.state_file.exists():
            with open(self.state_file) as f:
                return json.load(f)
        return {}

    def clear(self, scraper_name: str):
        """Clear scraper state."""
        state = self.load_all()
        if scraper_name in state:
            del state[scraper_name]
            with open(self.state_file, "w") as f:
                json.dump(state, f, indent=2)


class BaseScraper(ABC):
    """Base class for all scrapers."""

    def __init__(
        self,
        headless: bool = True,
        min_delay: float = 2.0,
        max_delay: float = 5.0,
        timeout: int = 30000,
    ):
        self.headless = headless
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.timeout = timeout
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        self.state = ScraperState()
        self._paused = False
        self._stop_requested = False

    @property
    @abstractmethod
    def name(self) -> str:
        """Scraper name for state management."""
        pass

    async def start(self):
        """Start the browser."""
        playwright = await async_playwright().start()
        self.browser = await playwright.chromium.launch(
            headless=self.headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ]
        )
        context = await self.browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )
        self.page = await context.new_page()
        self.page.set_default_timeout(self.timeout)

    async def stop(self):
        """Stop the browser."""
        if self.browser:
            await self.browser.close()
            self.browser = None
            self.page = None

    async def random_delay(self):
        """Wait a random amount of time to avoid detection."""
        delay = random.uniform(self.min_delay, self.max_delay)
        await asyncio.sleep(delay)

    async def check_for_captcha(self) -> bool:
        """Check if a CAPTCHA is present. Override in subclasses."""
        return False

    async def handle_captcha(self):
        """Handle CAPTCHA detection - pause and notify."""
        self._paused = True
        print("\n" + "=" * 60)
        print("CAPTCHA DETECTED!")
        print("Please solve the CAPTCHA in the browser window.")
        print("The scraper will resume automatically when solved.")
        print("=" * 60 + "\n")

        # Wait for CAPTCHA to be solved
        while await self.check_for_captcha():
            await asyncio.sleep(2)

        self._paused = False
        print("CAPTCHA solved! Resuming...")
        await self.random_delay()

    def save_progress(self, data: dict):
        """Save current progress for resume."""
        self.state.save(self.name, data)

    def load_progress(self) -> Optional[dict]:
        """Load saved progress."""
        return self.state.load(self.name)

    def clear_progress(self):
        """Clear saved progress."""
        self.state.clear(self.name)

    def request_stop(self):
        """Request the scraper to stop gracefully."""
        self._stop_requested = True

    @abstractmethod
    async def scrape_reviews(self, url: str, max_reviews: int = 100) -> list:
        """Scrape reviews from a URL. Must be implemented by subclasses."""
        pass

    @abstractmethod
    async def get_operator_urls(self, region: str = None) -> list[str]:
        """Get list of operator URLs to scrape."""
        pass
