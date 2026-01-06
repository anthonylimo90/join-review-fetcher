"""TripAdvisor scraper with enhanced anti-detection and updated selectors."""
import asyncio
import re
import random
from typing import Optional
from urllib.parse import urljoin, quote

from playwright.async_api import Page, ElementHandle

from .base import BaseScraper
from ..database.models import Review


class TripAdvisorScraper(BaseScraper):
    """Scraper for TripAdvisor safari reviews with anti-bot measures."""

    BASE_URL = "https://www.tripadvisor.com"

    # Safari search URLs for different regions
    SAFARI_SEARCH_URLS = {
        "kenya": "/Search?q=safari&geo=294206&searchNearby=false&ssrc=A",
        "tanzania": "/Search?q=safari&geo=293747&searchNearby=false&ssrc=A",
        "south_africa": "/Search?q=safari&geo=293740&searchNearby=false&ssrc=A",
        "botswana": "/Search?q=safari&geo=293764&searchNearby=false&ssrc=A",
        "namibia": "/Search?q=safari&geo=293825&searchNearby=false&ssrc=A",
        "zimbabwe": "/Search?q=safari&geo=293759&searchNearby=false&ssrc=A",
    }

    # Direct attraction category URLs (more reliable)
    ATTRACTION_URLS = {
        "kenya": "/Attractions-g294206-Activities-c61-Kenya.html",
        "tanzania": "/Attractions-g293747-Activities-c61-Tanzania.html",
        "south_africa": "/Attractions-g293740-Activities-c61-South_Africa.html",
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.min_delay = 4.0
        self.max_delay = 8.0

    @property
    def name(self) -> str:
        return "tripadvisor"

    async def start(self):
        """Start browser with stealth measures."""
        await super().start()

        if self.page:
            # Enhanced stealth scripts
            await self.page.add_init_script("""
                // Override webdriver
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });

                // Override plugins length
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                });

                // Override languages
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en', 'es']
                });

                // Override platform
                Object.defineProperty(navigator, 'platform', {
                    get: () => 'MacIntel'
                });

                // Override hardware concurrency
                Object.defineProperty(navigator, 'hardwareConcurrency', {
                    get: () => 8
                });

                // Remove automation indicators
                delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
                delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
                delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;

                // Override permissions
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
                );
            """)

    async def check_for_captcha(self) -> bool:
        """Check for CAPTCHA or blocking on TripAdvisor."""
        if not self.page:
            return False

        try:
            content = await self.page.content()
            url = self.page.url

            captcha_indicators = [
                "captcha", "recaptcha", "hcaptcha", "challenge",
                "security check", "verify you", "unusual traffic",
                "access denied", "blocked", "robot",
                "please verify", "human verification",
            ]

            # Check URL for captcha redirects
            if "captcha" in url.lower() or "challenge" in url.lower():
                return True

            return any(indicator in content.lower() for indicator in captcha_indicators)
        except Exception:
            return False

    async def _simulate_human(self):
        """Simulate human-like browsing behavior."""
        if not self.page:
            return

        try:
            # Random scroll
            for _ in range(random.randint(1, 3)):
                scroll = random.randint(100, 400)
                await self.page.evaluate(f"window.scrollBy(0, {scroll})")
                await asyncio.sleep(random.uniform(0.3, 0.8))

            # Random mouse movements
            viewport = self.page.viewport_size
            if viewport:
                x = random.randint(100, viewport["width"] - 100)
                y = random.randint(100, viewport["height"] - 100)
                await self.page.mouse.move(x, y)

            await asyncio.sleep(random.uniform(0.5, 1.5))
        except Exception:
            pass

    async def _accept_cookies(self):
        """Accept cookie consent if present."""
        try:
            # TripAdvisor cookie consent buttons
            cookie_selectors = [
                "button#onetrust-accept-btn-handler",
                "button[id*='accept']",
                "button:has-text('Accept')",
                "button:has-text('I Accept')",
                "button:has-text('Accept All')",
                "[data-testid='accept-cookies']",
            ]

            for selector in cookie_selectors:
                btn = await self.page.query_selector(selector)
                if btn:
                    await btn.click()
                    await asyncio.sleep(1)
                    break
        except Exception:
            pass

    async def get_operator_urls(self, region: str = "kenya") -> list[str]:
        """Get safari attraction/tour URLs for a region."""
        if not self.page:
            await self.start()

        urls = []

        # Try direct attraction URL first
        if region in self.ATTRACTION_URLS:
            listing_url = f"{self.BASE_URL}{self.ATTRACTION_URLS[region]}"
        else:
            listing_url = f"{self.BASE_URL}{self.SAFARI_SEARCH_URLS.get(region, self.SAFARI_SEARCH_URLS['kenya'])}"

        print(f"Loading: {listing_url}")

        try:
            await self.page.goto(listing_url, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(3)
            await self._accept_cookies()
            await self._simulate_human()

            if await self.check_for_captcha():
                await self.handle_captcha()

            page_num = 1
            max_pages = 5

            while page_num <= max_pages:
                if self._stop_requested:
                    break

                # TripAdvisor attraction link patterns
                link_selectors = [
                    "a[href*='/Attraction_Review-']",
                    "a[href*='/AttractionProductReview-']",
                    "div[data-automation='attraction'] a",
                    "[data-test-target='attraction-name'] a",
                    ".listing_title a",
                    ".result-title",
                ]

                for selector in link_selectors:
                    links = await self.page.query_selector_all(selector)
                    for link in links:
                        href = await link.get_attribute("href")
                        if href and ("Attraction_Review" in href or "AttractionProductReview" in href):
                            full_url = urljoin(self.BASE_URL, href)
                            if full_url not in urls:
                                urls.append(full_url)

                print(f"  Page {page_num}: Found {len(urls)} attractions")

                if len(urls) >= 50:  # Limit per region
                    break

                # Try pagination
                next_selectors = [
                    "a.next",
                    "a[data-page-number]",
                    "a:has-text('Next')",
                    ".pagination a.nav.next",
                    "[data-smoke-attr='pagination-next']",
                ]

                next_clicked = False
                for selector in next_selectors:
                    next_btn = await self.page.query_selector(selector)
                    if next_btn:
                        try:
                            await next_btn.click()
                            await self.random_delay()
                            await self._simulate_human()
                            page_num += 1
                            next_clicked = True

                            if await self.check_for_captcha():
                                await self.handle_captcha()
                            break
                        except Exception:
                            continue

                if not next_clicked:
                    break

        except Exception as e:
            print(f"Error getting operator URLs: {e}")

        return urls

    async def scrape_reviews(self, attraction_url: str, max_reviews: int = 100) -> list[Review]:
        """Scrape reviews from a TripAdvisor attraction page."""
        if not self.page:
            await self.start()

        reviews = []

        try:
            await self.page.goto(attraction_url, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(3)
            await self._accept_cookies()
            await self._simulate_human()

            if await self.check_for_captcha():
                await self.handle_captcha()

            # Get attraction name
            operator_name = await self._get_attraction_name()

            page_num = 1
            seen_reviews = set()

            while len(reviews) < max_reviews:
                if self._stop_requested:
                    break

                # Expand "Read more" buttons
                await self._expand_reviews()

                # Find review containers
                page_reviews = await self._extract_reviews(attraction_url, operator_name)

                for review in page_reviews:
                    review_key = f"{review.reviewer_name}:{review.text[:50] if review.text else ''}"
                    if review_key not in seen_reviews and review.text:
                        seen_reviews.add(review_key)
                        reviews.append(review)

                    if len(reviews) >= max_reviews:
                        break

                # Try to load more reviews
                if len(reviews) < max_reviews:
                    loaded_more = await self._load_more_reviews()
                    if not loaded_more:
                        break
                    page_num += 1

                    if await self.check_for_captcha():
                        await self.handle_captcha()

        except Exception as e:
            print(f"Error scraping {attraction_url}: {e}")

        return reviews

    async def _get_attraction_name(self) -> str:
        """Extract attraction name from page."""
        name_selectors = [
            "h1",
            "[data-automation='mainH1']",
            ".heading_title",
            "[data-test-target='attraction-name']",
        ]

        for selector in name_selectors:
            elem = await self.page.query_selector(selector)
            if elem:
                name = await elem.inner_text()
                return name.strip()

        return ""

    async def _expand_reviews(self):
        """Click 'Read more' buttons to expand review text."""
        try:
            expand_selectors = [
                "span.taLnk:has-text('Read more')",
                "span:has-text('Read more')",
                "[data-automation='readMore']",
                ".moreLink",
            ]

            for selector in expand_selectors:
                buttons = await self.page.query_selector_all(selector)
                for btn in buttons[:5]:  # Limit to avoid detection
                    try:
                        await btn.click()
                        await asyncio.sleep(0.2)
                    except Exception:
                        pass

        except Exception:
            pass

    async def _extract_reviews(self, url: str, operator_name: str) -> list[Review]:
        """Extract reviews from current page."""
        reviews = []

        # Multiple selector strategies for review containers
        container_selectors = [
            "[data-automation='reviewCard']",
            "[data-test-target='review-card']",
            ".review-container",
            "div[data-reviewid]",
            ".reviewSelector",
            "[class*='ReviewCard']",
            ".review",
        ]

        containers = []
        for selector in container_selectors:
            containers = await self.page.query_selector_all(selector)
            if containers:
                break

        for container in containers:
            review = await self._parse_review(container, url, operator_name)
            if review and review.text:
                reviews.append(review)

        return reviews

    async def _parse_review(
        self, container: ElementHandle, url: str, operator_name: str
    ) -> Optional[Review]:
        """Parse a single review from its container."""
        try:
            review = Review(
                source="tripadvisor",
                url=url,
                operator_name=operator_name,
            )

            text_content = await container.inner_text()

            # Reviewer name
            name_selectors = [
                ".member_info .username",
                "[class*='username']",
                "[data-automation='reviewerName']",
                ".memberOverlayLink",
                "a.ui_header_link",
            ]
            for selector in name_selectors:
                elem = await container.query_selector(selector)
                if elem:
                    review.reviewer_name = (await elem.inner_text()).strip()
                    break

            # Reviewer location
            location_selectors = [
                ".member_info .location",
                "[class*='userLocation']",
                ".userLoc",
                "[data-automation='reviewerLocation']",
            ]
            for selector in location_selectors:
                elem = await container.query_selector(selector)
                if elem:
                    loc = (await elem.inner_text()).strip()
                    review.reviewer_location = loc
                    review.reviewer_country = self._extract_country(loc)
                    break

            # Rating from bubble class
            rating_selectors = [
                "[class*='bubble_rating']",
                ".ui_bubble_rating",
                "[data-automation='bubbleRating']",
                "svg[class*='bubble']",
            ]
            for selector in rating_selectors:
                elem = await container.query_selector(selector)
                if elem:
                    class_attr = await elem.get_attribute("class") or ""
                    # Pattern: bubble_50 = 5.0, bubble_45 = 4.5, etc.
                    match = re.search(r"bubble_(\d+)", class_attr)
                    if match:
                        review.rating = float(match.group(1)) / 10
                        break

            # Review title
            title_selectors = [
                ".title",
                "[data-automation='reviewTitle']",
                ".quote a",
                "[class*='ReviewTitle']",
                ".noQuotes",
            ]
            for selector in title_selectors:
                elem = await container.query_selector(selector)
                if elem:
                    review.title = (await elem.inner_text()).strip()
                    break

            # Review text
            text_selectors = [
                ".entry .partial_entry",
                ".entry",
                "[data-automation='reviewText']",
                "[class*='ReviewText']",
                "q",
                ".prw_reviews_text_summary_hsx",
            ]
            for selector in text_selectors:
                elem = await container.query_selector(selector)
                if elem:
                    text = (await elem.inner_text()).strip()
                    # Clean up
                    text = re.sub(r"\s*Read more\s*$", "", text, flags=re.IGNORECASE)
                    text = re.sub(r"\s*\.{3,}\s*$", "", text)
                    review.text = text
                    break

            # Date
            date_selectors = [
                ".ratingDate",
                "[data-automation='reviewDate']",
                "[class*='TripDate']",
                ".prw_reviews_stay_date_hsx",
            ]
            for selector in date_selectors:
                elem = await container.query_selector(selector)
                if elem:
                    date_text = (await elem.inner_text()).strip()
                    # Extract date from patterns like "Reviewed January 2026" or "Date of experience: January 2026"
                    match = re.search(r"((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})", date_text, re.IGNORECASE)
                    if match:
                        if "experience" in date_text.lower():
                            review.travel_date = match.group(1)
                        else:
                            review.review_date = match.group(1)
                    break

            # Trip type
            trip_selectors = [
                "[class*='TripType']",
                ".recommend-titleInline",
                "[data-automation='tripType']",
            ]
            for selector in trip_selectors:
                elem = await container.query_selector(selector)
                if elem:
                    trip_text = (await elem.inner_text()).strip().lower()
                    review.trip_type = self._normalize_trip_type(trip_text)
                    break

            return review

        except Exception as e:
            print(f"Error parsing TripAdvisor review: {e}")
            return None

    async def _load_more_reviews(self) -> bool:
        """Try to load more reviews."""
        try:
            # Look for "Next" pagination or "Show more" button
            next_selectors = [
                "a.next",
                "a.nav.next",
                "[data-automation='paginationNext']",
                "a:has-text('Next')",
                "[data-smoke-attr='pagination-next']",
            ]

            for selector in next_selectors:
                btn = await self.page.query_selector(selector)
                if btn:
                    await btn.click()
                    await self.random_delay()
                    await self._simulate_human()
                    return True

            return False

        except Exception:
            return False

    def _extract_country(self, location: str) -> str:
        """Extract country from TripAdvisor location."""
        if not location:
            return ""

        location = location.strip()

        # TripAdvisor format: "City, State" or "City, Country"
        if "," in location:
            parts = [p.strip() for p in location.split(",")]
            return parts[-1]

        return location

    def _normalize_trip_type(self, trip_text: str) -> str:
        """Normalize trip type."""
        trip_text = trip_text.lower()

        if "solo" in trip_text:
            return "solo"
        elif "couple" in trip_text:
            return "couple"
        elif "family" in trip_text:
            return "family"
        elif "friend" in trip_text:
            return "friends"
        elif "business" in trip_text:
            return "business"

        return trip_text

    async def scrape_all(
        self,
        regions: list[str] = None,
        max_operators: int = 50,
        max_reviews_per_operator: int = 50,
        resume: bool = True,
    ) -> list[Review]:
        """Scrape reviews from multiple regions."""
        regions = regions or ["kenya", "tanzania"]
        all_reviews = []
        processed_urls = set()

        if resume:
            progress = self.load_progress()
            if progress:
                processed_urls = set(progress.get("processed_urls", []))
                print(f"Resuming from {len(processed_urls)} previously processed URLs")

        try:
            for region in regions:
                if self._stop_requested:
                    break

                print(f"\n=== Scraping {region.upper()} ===")

                operator_urls = await self.get_operator_urls(region)
                print(f"Found {len(operator_urls)} attractions in {region}")

                operators_per_region = max_operators // len(regions)

                for i, url in enumerate(operator_urls[:operators_per_region]):
                    if self._stop_requested:
                        break

                    if url in processed_urls:
                        continue

                    print(f"[{i+1}/{operators_per_region}] {url[:70]}...")

                    try:
                        reviews = await self.scrape_reviews(url, max_reviews=max_reviews_per_operator)
                        all_reviews.extend(reviews)
                        print(f"  Found {len(reviews)} reviews (total: {len(all_reviews)})")
                    except Exception as e:
                        print(f"  Error: {e}")

                    processed_urls.add(url)

                    self.save_progress({
                        "processed_urls": list(processed_urls),
                        "total_reviews": len(all_reviews),
                        "current_region": region,
                    })

                    await self.random_delay()

        finally:
            await self.stop()

        return all_reviews
