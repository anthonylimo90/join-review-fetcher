"""Base scraper class with common functionality."""
import asyncio
import random
import json
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, Callable, TypeVar
from datetime import datetime
from functools import wraps

from playwright.async_api import async_playwright, Page, Browser, TimeoutError as PlaywrightTimeout


# Custom exceptions
class ScraperError(Exception):
    """Base exception for scraper errors."""
    pass


class CaptchaTimeoutError(ScraperError):
    """Raised when CAPTCHA is not solved within timeout."""
    pass


class RateLimitError(ScraperError):
    """Raised when rate limiting is detected."""
    pass


class NetworkError(ScraperError):
    """Raised for retriable network errors."""
    pass


# Retry decorator for network operations
T = TypeVar('T')


def retry_on_network_error(max_retries: int = 3, base_delay: float = 2.0):
    """Decorator to retry async functions on network errors with exponential backoff."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            last_error = None
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except (PlaywrightTimeout, ConnectionError, ConnectionResetError,
                        OSError, asyncio.TimeoutError) as e:
                    last_error = e
                    if attempt < max_retries - 1:
                        delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                        print(f"  Network error (attempt {attempt + 1}/{max_retries}): {e}")
                        print(f"  Retrying in {delay:.1f}s...")
                        await asyncio.sleep(delay)
                    else:
                        raise NetworkError(f"Failed after {max_retries} retries: {e}") from e
            raise NetworkError(f"Failed after {max_retries} retries: {last_error}")
        return wrapper
    return decorator


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

    # CAPTCHA timeout in seconds (10 minutes)
    CAPTCHA_TIMEOUT = 600

    def __init__(
        self,
        headless: bool = True,
        min_delay: float = 2.0,
        max_delay: float = 5.0,
        timeout: int = 60000,  # Increased from 30s to 60s
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
        self._pause_requested = False  # For pause/resume functionality

        # Rate limiting state
        self.request_count = 0
        self.operators_scraped = 0
        self.last_request_time = 0.0
        self._rate_limit_multiplier = 1.0

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

    async def adaptive_delay(self):
        """Wait with adaptive delay based on request count - slower as we scrape more."""
        self.request_count += 1

        # Base delay
        base_delay = random.uniform(self.min_delay, self.max_delay)

        # Increase delay based on how many operators we've scraped
        # This helps avoid rate limiting on long runs
        if self.operators_scraped > 200:
            multiplier = 2.5
        elif self.operators_scraped > 100:
            multiplier = 2.0
        elif self.operators_scraped > 50:
            multiplier = 1.5
        else:
            multiplier = 1.0

        # Apply any rate limit multiplier from detected slowdowns
        multiplier *= self._rate_limit_multiplier

        delay = base_delay * multiplier

        # Add jitter to avoid patterns
        delay += random.uniform(0, 1)

        # Minimum time between requests (respect rate limits)
        time_since_last = time.time() - self.last_request_time
        if time_since_last < 1.0:
            delay += (1.0 - time_since_last)

        await asyncio.sleep(delay)
        self.last_request_time = time.time()

    def increase_rate_limit_delay(self):
        """Increase delay multiplier when rate limiting is suspected."""
        self._rate_limit_multiplier = min(5.0, self._rate_limit_multiplier * 1.5)
        print(f"  Rate limit detected, increasing delay multiplier to {self._rate_limit_multiplier:.1f}x")

    def reset_rate_limit_delay(self):
        """Reset delay multiplier after successful requests."""
        if self._rate_limit_multiplier > 1.0:
            self._rate_limit_multiplier = max(1.0, self._rate_limit_multiplier * 0.9)

    async def check_for_captcha(self) -> bool:
        """Check if a CAPTCHA is present. Override in subclasses."""
        return False

    async def handle_captcha(self) -> bool:
        """
        Handle CAPTCHA detection - pause and wait with timeout.

        Returns:
            True if CAPTCHA was solved, False if timeout (should skip operator)
        """
        self._paused = True
        print("\n" + "=" * 60)
        print("CAPTCHA DETECTED!")
        print("Please solve the CAPTCHA in the browser window.")
        print(f"Timeout: {self.CAPTCHA_TIMEOUT // 60} minutes")
        print("=" * 60 + "\n")

        start_time = time.time()

        # Wait for CAPTCHA to be solved with timeout
        while await self.check_for_captcha():
            elapsed = time.time() - start_time
            if elapsed > self.CAPTCHA_TIMEOUT:
                self._paused = False
                print(f"CAPTCHA timeout after {self.CAPTCHA_TIMEOUT // 60} minutes - skipping operator")
                return False

            # Show remaining time every 30 seconds
            if int(elapsed) % 30 == 0 and int(elapsed) > 0:
                remaining = (self.CAPTCHA_TIMEOUT - elapsed) / 60
                print(f"  Waiting for CAPTCHA... {remaining:.1f} minutes remaining")

            await asyncio.sleep(2)

        self._paused = False
        print("CAPTCHA solved! Resuming...")
        await self.random_delay()
        return True

    def save_progress(self, data: dict):
        """Save current progress for resume, including network state."""
        # Include network state for proper resume
        data.update({
            "rate_limit_multiplier": self._rate_limit_multiplier,
            "request_count": self.request_count,
            "operators_scraped": self.operators_scraped,
            "paused": self._pause_requested,
        })
        self.state.save(self.name, data)

    def load_progress(self) -> Optional[dict]:
        """Load saved progress and restore network state."""
        data = self.state.load(self.name)
        if data:
            # Restore network state
            self._rate_limit_multiplier = data.get("rate_limit_multiplier", 1.0)
            self.request_count = data.get("request_count", 0)
            self.operators_scraped = data.get("operators_scraped", 0)
        return data

    def clear_progress(self):
        """Clear saved progress."""
        self.state.clear(self.name)

    def request_stop(self):
        """Request the scraper to stop gracefully."""
        self._stop_requested = True

    def request_pause(self):
        """Request the scraper to pause gracefully (saves detailed checkpoint)."""
        self._pause_requested = True
        self._stop_requested = True  # Also set stop flag to break loops

    def is_pause_requested(self) -> bool:
        """Check if pause was requested (vs regular stop)."""
        return self._pause_requested

    async def restart_browser(self):
        """Restart browser to clear memory and reset state."""
        print("  Restarting browser to clear memory...")
        await self.stop()
        await asyncio.sleep(2)
        await self.start()
        print("  Browser restarted successfully")

    async def safe_goto(self, url: str, wait_until: str = "domcontentloaded",
                        max_retries: int = 3) -> bool:
        """
        Navigate to URL with retry logic for network errors.

        Returns:
            True if navigation succeeded, False if all retries failed
        """
        for attempt in range(max_retries):
            try:
                await self.page.goto(url, wait_until=wait_until, timeout=self.timeout)
                self.reset_rate_limit_delay()  # Successful request
                return True
            except PlaywrightTimeout as e:
                print(f"  Timeout loading {url} (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    self.increase_rate_limit_delay()
                    delay = 2 * (2 ** attempt) + random.uniform(0, 2)
                    await asyncio.sleep(delay)
                else:
                    return False
            except Exception as e:
                print(f"  Error loading {url}: {e} (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    delay = 2 * (2 ** attempt) + random.uniform(0, 2)
                    await asyncio.sleep(delay)
                else:
                    return False
        return False

    @abstractmethod
    async def scrape_reviews(self, url: str, max_reviews: int = 100) -> list:
        """Scrape reviews from a URL. Must be implemented by subclasses."""
        pass

    @abstractmethod
    async def get_operator_urls(self, region: str = None) -> list[str]:
        """Get list of operator URLs to scrape."""
        pass
