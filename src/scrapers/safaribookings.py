"""Safaribookings.com scraper - Enhanced with robust parsing and validation."""
import asyncio
import json
import random
import re
from typing import Optional, AsyncIterator
from urllib.parse import urljoin

from playwright.async_api import Page, ElementHandle

from .base import BaseScraper
from .country_codes import COUNTRY_CODES, get_country_name, get_region
from .validation import ReviewValidator, ParsingErrorTracker, ParseResult
from ..database.models import Review


# Multi-strategy regex patterns for reviewer line parsing
# Try in order - first match wins, with decreasing confidence
# Note: Name pattern now requires proper name format (First Last or First L.)
# PRE-COMPILED for performance (compiled once at module load)
REVIEWER_PATTERNS = [
    # Pattern 1: Standard format with en-dash - strict name (First Last format)
    (re.compile(r"\n([A-Z][a-z]+(?:\s+[A-Z][a-z\.]+)?)\s+–\s+([A-Z]{2})\s+Visited:\s*(\w+\s+\d{4})\s+Reviewed:\s*([A-Za-z]+\s+\d+,?\s*\d{4})"), 1.0),
    # Pattern 2: Hyphen instead of en-dash
    (re.compile(r"\n([A-Z][a-z]+(?:\s+[A-Z][a-z\.]+)?)\s+-\s+([A-Z]{2})\s+Visited:\s*(\w+\s+\d{4})\s+Reviewed:\s*([A-Za-z]+\s+\d+,?\s*\d{4})"), 0.95),
    # Pattern 3: Em-dash
    (re.compile(r"\n([A-Z][a-z]+(?:\s+[A-Z][a-z\.]+)?)\s+—\s+([A-Z]{2})\s+Visited:\s*(\w+\s+\d{4})\s+Reviewed:\s*([A-Za-z]+\s+\d+,?\s*\d{4})"), 0.95),
    # Pattern 4: More flexible name (allows hyphens, apostrophes in names)
    (re.compile(r"\n([A-Z][a-z]+(?:[-'][A-Z]?[a-z]+)?(?:\s+[A-Z][a-z\.]+)?)\s+[–—-]\s+([A-Z]{2})\s+Visited:\s*(\w+\s+\d{4})"), 0.85),
    # Pattern 5: Name on separate line (fallback)
    (re.compile(r"\n([A-Z][a-z]+(?:\s+[A-Z][a-z\.]+)?)\n\s*([A-Z]{2})\s+Visited:\s*(\w+\s+\d{4})"), 0.75),
]

# Wildlife keywords for extraction
WILDLIFE_KEYWORDS = {
    "big_five": ["lion", "elephant", "leopard", "rhino", "rhinoceros", "buffalo", "cape buffalo"],
    "predators": ["cheetah", "wild dog", "hyena", "jackal", "serval", "caracal", "crocodile"],
    "herbivores": ["giraffe", "zebra", "wildebeest", "gnu", "antelope", "impala", "gazelle",
                   "kudu", "eland", "waterbuck", "hartebeest", "topi", "oryx", "hippo", "hippopotamus"],
    "primates": ["baboon", "monkey", "vervet", "colobus", "chimpanzee", "gorilla"],
    "birds": ["ostrich", "flamingo", "eagle", "vulture", "secretary bird", "hornbill",
              "kingfisher", "bee-eater", "roller", "stork", "heron", "pelican"],
}

# Pre-compiled wildlife regex for single-pass extraction (much faster than looping)
_ALL_WILDLIFE = [animal for animals in WILDLIFE_KEYWORDS.values() for animal in animals]
WILDLIFE_REGEX = re.compile(
    r'\b(' + '|'.join(re.escape(a) for a in _ALL_WILDLIFE) + r')s?\b',
    re.IGNORECASE
)

# Safari park names for extraction
SAFARI_PARKS = [
    "masai mara", "maasai mara", "serengeti", "ngorongoro", "amboseli", "tsavo",
    "kruger", "chobe", "okavango", "etosha", "hwange", "south luangwa",
    "queen elizabeth", "bwindi", "lake nakuru", "lake manyara", "tarangire",
    "samburu", "ol pejeta", "laikipia", "lewa", "meru", "selous", "ruaha",
    "mikumi", "katavi", "gombe", "mahale", "victoria falls", "livingstone",
]

# Pre-compiled parks regex for single-pass extraction
PARKS_REGEX = re.compile(
    r'\b(' + '|'.join(re.escape(p) for p in SAFARI_PARKS) + r')\b',
    re.IGNORECASE
)

# Trip type classification keywords
TRIP_TYPES = {
    "solo": ["solo", "alone", "single", "by myself", "on my own"],
    "couple": ["couple", "honeymoon", "romantic", "wife", "husband", "partner", "anniversary", "newlywed"],
    "family": ["family", "kids", "children", "daughter", "son", "parents", "grandparents", "grandchildren"],
    "friends": ["friends", "buddies", "mates", "group of friends"],
    "group": ["group", "tour group", "organized tour", "party of"],
    "first_safari": ["first safari", "first time", "bucket list", "dream trip", "always wanted"],
    "repeat": ["return", "back again", "second safari", "third safari", "many safaris", "regular visitor"],
    "photography": ["photography", "photographer", "photos", "camera", "wildlife photography"],
    "birdwatching": ["birding", "birdwatching", "bird watching", "ornithology"],
}

# Pre-compiled guide name extraction patterns
GUIDE_PATTERNS = [
    re.compile(r"(?:our|the|my)\s+(?:guide|driver|ranger)[,\s]+([A-Z][a-z]+)", re.IGNORECASE),
    re.compile(r"([A-Z][a-z]+)\s+(?:was|is)\s+(?:our|the|my|an?\s+(?:amazing|excellent|great|fantastic|wonderful))\s+(?:guide|driver|ranger)", re.IGNORECASE),
    re.compile(r"(?:guide|driver|ranger)\s+(?:named|called)\s+([A-Z][a-z]+)", re.IGNORECASE),
    re.compile(r"(?:thanks?\s+(?:to\s+)?|shout\s*out\s+(?:to\s+)?)([A-Z][a-z]+)", re.IGNORECASE),
    re.compile(r"([A-Z][a-z]+)\s+(?:guided|drove|took)\s+us", re.IGNORECASE),
]

# Common false positive names to filter out
GUIDE_NAME_BLACKLIST = frozenset(["the", "our", "was", "had", "very", "really", "great", "amazing"])


class SafaribookingsScraper(BaseScraper):
    """Scraper for Safaribookings.com safari reviews with enhanced data extraction."""

    BASE_URL = "https://www.safaribookings.com"
    MIN_TEXT_LENGTH = 10  # Reduced from 30 to capture more reviews

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.validator = ReviewValidator()
        self.error_tracker = ParsingErrorTracker()
        self._cookies_dismissed = False  # Track if we've already dismissed cookies this session

    @property
    def name(self) -> str:
        return "safaribookings"

    # ==================== Extraction Methods ====================

    def extract_wildlife_sightings(self, text: str) -> list[str]:
        """Extract wildlife sightings from review text using pre-compiled regex."""
        if not text:
            return []

        # Use pre-compiled regex for single-pass extraction (much faster)
        matches = WILDLIFE_REGEX.findall(text)
        # Return unique sightings, preserving lowercase for consistency
        return list(set(match.lower() for match in matches))

    def extract_parks_visited(self, text: str) -> list[str]:
        """Extract safari park names from review text using pre-compiled regex."""
        if not text:
            return []

        # Use pre-compiled regex for single-pass extraction
        matches = PARKS_REGEX.findall(text)
        # Return unique parks with proper capitalization
        return list(set(match.title() for match in matches))

    def extract_guide_names(self, text: str) -> list[str]:
        """Extract guide names mentioned in review text using pre-compiled patterns."""
        if not text:
            return []

        names = []
        for pattern in GUIDE_PATTERNS:
            matches = pattern.findall(text)
            for match in matches:
                name = match.strip().title()
                # Filter out common false positives
                if name and len(name) > 2 and name not in names:
                    if name.lower() not in GUIDE_NAME_BLACKLIST:
                        names.append(name)

        return names

    def extract_age_range(self, text: str) -> str:
        """Extract reviewer age range from text."""
        if not text:
            return ""

        patterns = [
            r"(\d{2})\s*[-–]\s*(\d{2})\s*(?:years?|yrs?)",
            r"(?:age|aged?)\s*(?:group)?[:\s]*(\d{2})\s*[-–]\s*(\d{2})",
            r"(\d{2})\s*to\s*(\d{2})\s*years?",
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return f"{match.group(1)}-{match.group(2)}"

        return ""

    def extract_safari_duration(self, text: str) -> Optional[int]:
        """Extract safari duration in days from text."""
        if not text:
            return None

        patterns = [
            r"(\d+)\s*(?:day|night)s?\s+(?:safari|trip|tour)",
            r"(?:safari|trip|tour)\s+(?:of\s+)?(\d+)\s*(?:day|night)s?",
            r"(\d+)\s*[-–]\s*(?:day|night)\s+(?:safari|trip|tour)",
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    return int(match.group(1))
                except ValueError:
                    continue

        return None

    def classify_trip_type(self, text: str) -> str:
        """Classify trip type from review text."""
        if not text:
            return ""

        text_lower = text.lower()

        for trip_type, keywords in TRIP_TYPES.items():
            for keyword in keywords:
                if keyword in text_lower:
                    return trip_type

        return ""

    async def check_for_captcha(self) -> bool:
        """Check for actual CAPTCHA or blocking (not cookie popups)."""
        if not self.page:
            return False
        try:
            # First, try to dismiss any cookie popups
            await self._dismiss_cookie_popup()

            # Check for actual CAPTCHA elements
            captcha_selectors = [
                "iframe[src*='captcha']",
                "iframe[src*='recaptcha']",
                "iframe[src*='hcaptcha']",
                ".g-recaptcha",
                "#captcha",
                "[class*='captcha']",
            ]

            for selector in captcha_selectors:
                elem = await self.page.query_selector(selector)
                if elem:
                    return True

            # Check URL for captcha redirects
            url = self.page.url.lower()
            if "captcha" in url or "challenge" in url or "blocked" in url:
                return True

            # Check for blocking messages (but not cookie consent)
            content = await self.page.content()
            content_lower = content.lower()

            # Only flag as CAPTCHA if we see actual blocking indicators
            # AND we don't see normal page content
            blocking_indicators = [
                "access denied",
                "too many requests",
                "rate limit",
                "please verify you are human",
                "security check required",
            ]

            has_blocking = any(indicator in content_lower for indicator in blocking_indicators)

            # Check if normal content is present (reviews, operators, etc.)
            has_normal_content = "safari" in content_lower and ("review" in content_lower or "operator" in content_lower)

            return has_blocking and not has_normal_content

        except Exception:
            return False

    async def _dismiss_cookie_popup(self, page: Page = None):
        """Dismiss cookie consent popups. Skips if already dismissed this session."""
        # Skip if we've already successfully dismissed cookies
        if self._cookies_dismissed:
            return

        target_page = page or self.page

        try:
            cookie_selectors = [
                # Cookiebot (used by Safaribookings)
                "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
                "#CybotCookiebotDialogBodyButtonAccept",
                # OneTrust
                "button#onetrust-accept-btn-handler",
                # Generic patterns
                "button[id*='accept' i]",
                "button[id*='Accept']",
                "button:has-text('Accept All')",
                "button:has-text('Accept all')",
                "button:has-text('Accept Cookies')",
                "button:has-text('I Accept')",
                "button:has-text('Allow All')",
                "button:has-text('Allow all')",
                "a:has-text('Accept')",
                ".cookie-accept",
                "[data-action='accept']",
                ".cc-btn.cc-allow",
                "button.cc-allow",
            ]

            for selector in cookie_selectors:
                try:
                    btn = await target_page.query_selector(selector)
                    if btn:
                        await btn.click()
                        await asyncio.sleep(0.5)  # Reduced from 1s
                        self._cookies_dismissed = True
                        print("  Dismissed cookie popup")
                        return
                except Exception:
                    continue

        except Exception:
            pass

    async def get_operator_urls(self, max_pages: int = 10) -> list[str]:
        """Get list of safari operator URLs from Safaribookings.

        Uses fast HTTP method by default (10-20x faster), with browser fallback.
        """
        import sys

        # Try fast HTTP method first (much faster than browser)
        try:
            from .http_helper import fetch_operator_urls_fast, is_http_available
            if is_http_available():
                print(f"[HTTP] Attempting fast HTTP operator discovery...", flush=True)
                sys.stdout.flush()
                operators = await fetch_operator_urls_fast(
                    base_url=self.BASE_URL,
                    max_pages=max_pages,
                    timeout=30.0
                )
                if operators:
                    print(f"[HTTP] Success! Found {len(operators)} operators via HTTP", flush=True)
                    sys.stdout.flush()
                    return operators
                print("[HTTP] No operators found, falling back to browser...", flush=True)
                sys.stdout.flush()
        except ImportError as e:
            print(f"[HTTP] Dependencies not available ({e}), using browser...", flush=True)
            sys.stdout.flush()
        except Exception as e:
            import traceback
            print(f"[HTTP] Failed with error: {type(e).__name__}: {e}", flush=True)
            print(f"[HTTP] Traceback: {traceback.format_exc()}", flush=True)
            sys.stdout.flush()

        # Fallback to browser-based method
        print("[HTTP] Using browser fallback for operator discovery...", flush=True)
        sys.stdout.flush()
        return await self._get_operator_urls_browser(max_pages)

    async def _get_operator_urls_browser(self, max_pages: int = 10) -> list[str]:
        """Get operator URLs using browser (fallback method)."""
        if not self.page:
            await self.start()

        operators = []
        base_url = f"{self.BASE_URL}/operators"

        print(f"Loading operators page: {base_url}", flush=True)
        # Use safe_goto with retry logic
        if not await self.safe_goto(base_url):
            print(f"Failed to load operators page after retries")
            return operators
        await asyncio.sleep(2)  # Wait for page to fully load

        # Dismiss cookie popup first
        await self._dismiss_cookie_popup()
        await self.random_delay()

        if await self.check_for_captcha():
            if not await self.handle_captcha():
                print("CAPTCHA timeout on operators page")
                return operators

        page_num = 1
        while page_num <= max_pages:
            if self._stop_requested:
                break

            # Safaribookings uses li[data-id] a with full URLs like safaribookings.com/p{id}
            operator_links = await self.page.query_selector_all("li[data-id] a")

            if not operator_links:
                # Try alternative: any link containing /p followed by digits
                operator_links = await self.page.query_selector_all("a[href*='/p']")

            for link in operator_links:
                href = await link.get_attribute("href")
                if href and re.search(r"/p\d+", href):
                    # Normalize URL
                    if href.startswith("http"):
                        full_url = href
                    else:
                        full_url = urljoin(self.BASE_URL, href)
                    if full_url not in operators and "safaribookings.com/p" in full_url:
                        operators.append(full_url)

            print(f"  Page {page_num}: Found {len(operators)} operators so far", flush=True)

            # Look for pagination - "Next" link or page numbers
            next_link = await self.page.query_selector("a:has-text('Next'), a:has-text('»')")

            if not next_link:
                # Try finding next page number link
                next_page_link = await self.page.query_selector(f"a[href*='page={page_num + 1}']")
                if next_page_link:
                    next_link = next_page_link

            if next_link and page_num < max_pages:
                try:
                    await next_link.click()
                    await self.adaptive_delay()  # Use adaptive delay
                    page_num += 1

                    if await self.check_for_captcha():
                        if not await self.handle_captcha():
                            print("  CAPTCHA timeout during pagination")
                            break
                except Exception as e:
                    print(f"  Pagination error: {e}")
                    # Retry once after a longer delay
                    await asyncio.sleep(5)
                    try:
                        await next_link.click()
                        page_num += 1
                    except Exception:
                        break
            else:
                break

            # Save checkpoint every page (more frequent)
            self.save_progress({
                "operator_urls": operators,
                "last_page": page_num,
            })

        return operators

    async def scrape_reviews(self, operator_url: str, max_reviews: int = 100) -> list[Review]:
        """Scrape reviews for a specific operator."""
        if not self.page:
            await self.start()

        reviews = []

        # Extract operator ID from URL (e.g., /p2606 -> 2606)
        match = re.search(r"/p(\d+)", operator_url)
        if not match:
            print(f"Could not extract operator ID from {operator_url}")
            return reviews

        operator_id = match.group(1)
        reviews_url = f"{self.BASE_URL}/reviews/p{operator_id}"

        print(f"  Loading reviews page: {reviews_url}", flush=True)
        # Use safe_goto with retry logic
        if not await self.safe_goto(reviews_url):
            print(f"  Failed to load {reviews_url} after retries")
            return reviews
        await asyncio.sleep(2)  # Wait for content to load

        print("  Page loaded, dismissing cookies...")
        # Dismiss cookie popup first
        await self._dismiss_cookie_popup()

        print("  Checking for CAPTCHA...")
        if await self.check_for_captcha():
            if not await self.handle_captcha():
                # CAPTCHA timeout - skip this operator
                print("  Skipping operator due to CAPTCHA timeout")
                return reviews

        # Get operator name from h1
        operator_name = ""
        try:
            h1 = await self.page.query_selector("h1")
            if h1:
                operator_name = await h1.inner_text()
                # Clean up - remove "Reviews" suffix if present
                operator_name = re.sub(r"\s*reviews?\s*$", "", operator_name, flags=re.IGNORECASE).strip()
                print(f"  Operator: {operator_name}")
        except Exception:
            pass

        page_num = 1
        seen_urls = set()

        print("  Extracting reviews...")
        while len(reviews) < max_reviews:
            if self._stop_requested:
                break

            # Parse reviews from current page using text extraction
            page_reviews = await self._extract_reviews_from_page(
                operator_url, operator_name
            )
            print(f"  Page {page_num}: Found {len(page_reviews)} reviews on this page")

            if not page_reviews:
                # No reviews found, exit the loop
                print("  No reviews found on this page, stopping pagination")
                break

            for review in page_reviews:
                review_key = f"{review.reviewer_name}:{review.text[:50] if review.text else ''}"
                if review_key not in seen_urls and review.text:
                    seen_urls.add(review_key)
                    reviews.append(review)

                if len(reviews) >= max_reviews:
                    break

            # Check for next page - look for pagination links
            next_link = await self.page.query_selector(
                "a:has-text('Next'), a:has-text('Page " + str(page_num + 1) + "'), "
                "a.pagination-next, a[rel='next']"
            )

            if next_link and len(reviews) < max_reviews:
                try:
                    print(f"  Navigating to page {page_num + 1}...")
                    await next_link.click()
                    await self.adaptive_delay()  # Use adaptive delay
                    page_num += 1
                except Exception as e:
                    print(f"  Pagination error: {e}")
                    break
            else:
                print("  No more pages or max reviews reached")
                break

            # Save checkpoint after EVERY page (for pause/resume support)
            self.save_progress({
                "current_url": operator_url,
                "current_operator_name": operator_name,
                "reviews_count": len(reviews),
                "page": page_num,
                "reviews_for_current_operator": len(reviews),
            })

        print(f"  Total reviews extracted: {len(reviews)}")
        return reviews

    async def scrape_reviews_with_page(
        self, operator_url: str, page: Page, max_reviews: int = 100,
        existing_urls: set[str] = None
    ) -> list[Review]:
        """Scrape reviews using a provided page (for parallel execution).

        This method is used by parallel workers that each have their own browser context.

        Args:
            operator_url: URL of the operator
            page: Playwright page to use
            max_reviews: Maximum reviews to scrape
            existing_urls: Set of review URLs already in database (to detect duplicates early)
        """
        reviews = []
        existing_urls = existing_urls or set()
        consecutive_duplicates = 0
        MAX_CONSECUTIVE_DUPLICATES = 10  # Stop if we hit this many duplicates in a row

        # Extract operator ID from URL (e.g., /p2606 -> 2606)
        match = re.search(r"/p(\d+)", operator_url)
        if not match:
            print(f"Could not extract operator ID from {operator_url}")
            return reviews

        operator_id = match.group(1)
        reviews_url = f"{self.BASE_URL}/reviews/p{operator_id}"

        # Navigate using the provided page
        try:
            await page.goto(reviews_url, wait_until="domcontentloaded", timeout=self.timeout)
            await asyncio.sleep(0.5)  # Reduced wait time
        except Exception as e:
            print(f"  Failed to load {reviews_url}: {e}")
            return reviews

        # Dismiss cookies on this page
        await self._dismiss_cookie_popup(page)

        # Get operator name from h1
        operator_name = ""
        try:
            h1 = await page.query_selector("h1")
            if h1:
                operator_name = await h1.inner_text()
                operator_name = re.sub(r"\s*reviews?\s*$", "", operator_name, flags=re.IGNORECASE).strip()
        except Exception:
            pass

        page_num = 1
        seen_urls = set()

        while len(reviews) < max_reviews:
            if self._stop_requested:
                break

            # Parse reviews from current page using the provided page
            page_reviews = await self._parse_reviews_from_text_with_page(
                page, operator_url, operator_name
            )

            if not page_reviews:
                break

            for review in page_reviews:
                review_key = f"{review.reviewer_name}:{review.text[:50] if review.text else ''}"
                if review_key not in seen_urls and review.text:
                    seen_urls.add(review_key)

                    # Check if this review already exists in database
                    if review.url in existing_urls:
                        consecutive_duplicates += 1
                        if consecutive_duplicates >= MAX_CONSECUTIVE_DUPLICATES:
                            # Stop early - we've hit too many consecutive duplicates
                            # This means we're likely into reviews we already have
                            return reviews
                    else:
                        consecutive_duplicates = 0  # Reset counter on new review
                        reviews.append(review)

                if len(reviews) >= max_reviews:
                    break

            # If we hit max duplicates, stop pagination
            if consecutive_duplicates >= MAX_CONSECUTIVE_DUPLICATES:
                break

            # Check for next page
            next_link = await page.query_selector(
                "a:has-text('Next'), a:has-text('Page " + str(page_num + 1) + "'), "
                "a.pagination-next, a[rel='next']"
            )

            if next_link and len(reviews) < max_reviews:
                try:
                    await next_link.click()
                    await asyncio.sleep(random.uniform(self.min_delay, self.max_delay))
                    page_num += 1
                except Exception as e:
                    break
            else:
                break

        return reviews

    async def _parse_reviews_from_text_with_page(
        self, page: Page, operator_url: str, operator_name: str
    ) -> list[Review]:
        """Parse reviews from page text using provided page (for parallel execution)."""
        reviews = []

        try:
            body = await page.query_selector("body")
            if not body:
                return reviews

            full_text = await body.inner_text()

            # Try multiple patterns with fallbacks
            all_matches = []
            used_positions = set()

            for pattern, confidence in REVIEWER_PATTERNS:
                try:
                    matches = list(pattern.finditer(full_text))
                    for m in matches:
                        start_pos = m.start()
                        if start_pos not in used_positions:
                            all_matches.append((m, confidence, start_pos))
                            used_positions.add(start_pos)
                except Exception:
                    continue

            # Sort by position
            all_matches.sort(key=lambda x: x[2])

            # Extract reviews from matches
            for i, (match, confidence, pos) in enumerate(all_matches):
                try:
                    groups = match.groups()
                    if len(groups) >= 3:
                        name = groups[0].strip()
                        country_code = groups[1].upper() if groups[1] else ""
                        travel_date_str = groups[2] if len(groups) > 2 else ""
                        review_date_str = groups[3] if len(groups) > 3 else ""

                        # Get review text
                        end_pos = all_matches[i + 1][2] if i + 1 < len(all_matches) else len(full_text)
                        review_text = full_text[match.end():end_pos].strip()

                        # Clean up review text
                        review_text = re.sub(r'\n{3,}', '\n\n', review_text)
                        review_text = re.sub(r'Share this review.*$', '', review_text, flags=re.IGNORECASE)
                        review_text = review_text.strip()

                        if len(review_text) >= self.MIN_TEXT_LENGTH:
                            # Generate unique review URL with reviewer name
                            reviewer_slug = name.replace(' ', '-').lower()
                            review_url = f"{operator_url}#review-{i+1}-{reviewer_slug}"

                            review = Review(
                                source="safaribookings",
                                url=review_url,
                                operator_name=operator_name,
                                reviewer_name=name,
                                reviewer_country=get_country_name(country_code),
                                text=review_text,
                                wildlife_sightings=json.dumps(self.extract_wildlife_sightings(review_text)),
                                parks_visited=json.dumps(self.extract_parks_visited(review_text)),
                                guide_names_mentioned=json.dumps(self.extract_guide_names(review_text)),
                            )
                            reviews.append(review)
                except Exception:
                    continue

        except Exception as e:
            print(f"    Parse error: {e}")

        return reviews

    async def _extract_reviews_from_page(
        self, operator_url: str, operator_name: str
    ) -> list[Review]:
        """Extract reviews by parsing page content."""
        # Safaribookings doesn't use well-structured review containers,
        # so we use text-based parsing which works more reliably
        return await self._parse_reviews_from_text(operator_url, operator_name)

    async def _parse_review_container(
        self, container: ElementHandle, operator_url: str, operator_name: str
    ) -> Optional[Review]:
        """Parse a review from a container element."""
        try:
            review = Review(
                source="safaribookings",
                url=operator_url,
                operator_name=operator_name,
            )

            # Get all text content
            text_content = await container.inner_text()

            # Extract reviewer name - usually bold/strong at the start
            name_elem = await container.query_selector("strong, b, .name, .reviewer, .author")
            if name_elem:
                review.reviewer_name = (await name_elem.inner_text()).strip()

            # Extract country - look for flag images or country codes
            flag_img = await container.query_selector("img[src*='flag'], img[alt*='flag']")
            if flag_img:
                alt = await flag_img.get_attribute("alt") or ""
                review.reviewer_country = alt.strip()
            else:
                # Look for country codes like "US", "UK", "DE"
                country_match = re.search(r"\b([A-Z]{2})\b", text_content)
                if country_match:
                    code = country_match.group(1)
                    review.reviewer_country = COUNTRY_CODES.get(code, code)

            # Extract rating - look for "_X_/5" pattern
            rating_match = re.search(r"_?(\d+(?:\.\d+)?)\s*_?/\s*5", text_content)
            if rating_match:
                review.rating = float(rating_match.group(1))

            # Extract title - usually h3, h4, h5 or bold text after rating
            title_elem = await container.query_selector("h3, h4, h5, .title, .headline")
            if title_elem:
                review.title = (await title_elem.inner_text()).strip()

            # Extract review text - main paragraph content
            text_elem = await container.query_selector("p, .text, .content, .body, .description")
            if text_elem:
                review.text = (await text_elem.inner_text()).strip()
            elif not review.text:
                # Fallback: get text content and clean it
                review.text = self._clean_review_text(text_content)

            # Extract travel date - "Visited: Month Year"
            date_match = re.search(r"(?:Visited|Travel(?:ed)?)[:\s]+(\w+\s+\d{4})", text_content, re.IGNORECASE)
            if date_match:
                review.travel_date = date_match.group(1)

            # Extract experience level
            exp_match = re.search(r"(?:Experience level|First safari|Repeat)[:\s]+(\w+(?:\s+\w+)?)", text_content, re.IGNORECASE)
            if exp_match:
                exp = exp_match.group(1).lower()
                if "first" in exp:
                    review.trip_type = "first_safari"

            return review

        except Exception as e:
            print(f"Error parsing review container: {e}")
            return None

    async def _parse_reviews_from_text(
        self, operator_url: str, operator_name: str
    ) -> list[Review]:
        """Parse reviews from page text using multi-strategy parsing."""
        reviews = []

        try:
            body = await self.page.query_selector("body")
            if not body:
                print("    Could not find body element")
                return reviews

            full_text = await body.inner_text()
            print(f"    Got page text: {len(full_text)} characters")

            # Try multiple patterns with fallbacks
            all_matches = []
            used_positions = set()

            for pattern, confidence in REVIEWER_PATTERNS:
                try:
                    matches = list(re.finditer(pattern, full_text))
                    for match in matches:
                        # Avoid duplicate matches at same position
                        if match.start() not in used_positions:
                            all_matches.append({
                                'match': match,
                                'confidence': confidence,
                                'pattern': pattern[:50],  # For debugging
                            })
                            used_positions.add(match.start())
                except re.error:
                    continue

            # Sort by position in text
            all_matches.sort(key=lambda x: x['match'].start())
            print(f"    Multi-strategy parsing found {len(all_matches)} reviewer matches")

            for i, match_info in enumerate(all_matches):
                match = match_info['match']
                confidence = match_info['confidence']
                warnings = []

                try:
                    # Create unique URL for each review
                    reviewer_name = match.group(1).strip()
                    review_url = f"{operator_url}#review-{i+1}-{reviewer_name.replace(' ', '-').lower()}"

                    review = Review(
                        source="safaribookings",
                        url=review_url,
                        operator_name=operator_name,
                        parsing_confidence=confidence,
                    )

                    # Extract from regex groups - clean up reviewer name
                    # Remove any feedback section text that may have been captured
                    clean_name = reviewer_name
                    for noise in ["yes", "no", "link to this review", "\n"]:
                        clean_name = clean_name.replace(noise, "").replace(noise.title(), "")
                    clean_name = " ".join(clean_name.split()).strip()
                    review.reviewer_name = clean_name
                    country_code = match.group(2).upper()
                    review.reviewer_country = get_country_name(country_code)

                    # Travel date (group 3)
                    if len(match.groups()) >= 3:
                        review.travel_date = match.group(3)

                    # Review date (group 4 if exists)
                    if len(match.groups()) >= 4 and match.group(4):
                        review.review_date = match.group(4)

                    # Get text block between this match and next
                    start_pos = match.end()
                    end_pos = all_matches[i + 1]['match'].start() if i + 1 < len(all_matches) else len(full_text)
                    block = full_text[start_pos:end_pos]

                    # Store raw block for debugging
                    review.raw_text_block = block[:2000] if len(block) > 2000 else block

                    # Extract rating
                    rating_match = re.search(r"\s(\d+(?:\.\d+)?)\s*/\s*5", block)
                    if rating_match:
                        review.rating = float(rating_match.group(1))

                    # Extract experience level and trip type
                    exp_match = re.search(r"Experience level:\s*([^\n|]+)", block)
                    if exp_match:
                        exp = exp_match.group(1).strip().lower()
                        if "first" in exp:
                            review.trip_type = "first_safari"
                        elif "2-5" in exp or "repeat" in exp or "6+" in exp:
                            review.trip_type = "repeat"

                    # Extract age range from block
                    age_match = re.search(r"(\d{2})\s*[-–]\s*(\d{2})\s*years", block)
                    if age_match:
                        review.age_range = f"{age_match.group(1)}-{age_match.group(2)}"

                    # Parse title and text from block structure
                    lines = block.strip().split("\n")

                    # Find the rating line position
                    rating_line_idx = -1
                    for idx, line in enumerate(lines):
                        if re.match(r"^\s*\d+\s*/\s*5\s*$", line.strip()):
                            rating_line_idx = idx
                            break

                    # Title is usually the line just before rating
                    if rating_line_idx > 0:
                        for idx in range(rating_line_idx - 1, -1, -1):
                            line = lines[idx].strip()
                            if line and not any(x in line.lower() for x in [
                                "email", "experience level", "years of age", "|"
                            ]):
                                review.title = line
                                break

                    # Text is everything after rating until feedback section
                    text_lines = []
                    if rating_line_idx >= 0:
                        for idx in range(rating_line_idx + 1, len(lines)):
                            line = lines[idx].strip()

                            # Stop at feedback section
                            if "was this review helpful" in line.lower():
                                break
                            if line.lower() in ["yes", "no"] or "link to this review" in line.lower():
                                break

                            # Accept all lines (reduced filtering)
                            if len(line) > 5:
                                text_lines.append(line)

                    review.text = " ".join(text_lines)

                    # === NEW: Extract additional fields ===

                    # Wildlife sightings
                    wildlife = self.extract_wildlife_sightings(review.text)
                    if wildlife:
                        review.wildlife_sightings = json.dumps(wildlife)

                    # Parks visited
                    parks = self.extract_parks_visited(review.text)
                    if parks:
                        review.parks_visited = json.dumps(parks)

                    # Guide names
                    guides = self.extract_guide_names(review.text)
                    if guides:
                        review.guide_names_mentioned = json.dumps(guides)

                    # Safari duration
                    duration = self.extract_safari_duration(review.text)
                    if duration:
                        review.safari_duration_days = duration

                    # Trip type from text (if not set from experience level)
                    if not review.trip_type:
                        review.trip_type = self.classify_trip_type(review.text)

                    # Validate and track
                    is_valid, validation_warnings = self.validator.validate(review)
                    warnings.extend(validation_warnings)

                    # Record parsing result
                    self.error_tracker.record_attempt(ParseResult(
                        success=True,
                        review=review,
                        confidence=confidence,
                        warnings=warnings,
                        raw_block=block[:500],
                        strategy_used=match_info['pattern'],
                    ))

                    # Accept reviews with text >= MIN_TEXT_LENGTH (reduced from 30)
                    if review.text and len(review.text) >= self.MIN_TEXT_LENGTH:
                        reviews.append(review)
                    elif review.text:
                        # Still accept but flag as short
                        warnings.append(f"short_text:{len(review.text)}")
                        review.parse_warnings = json.dumps(warnings)
                        reviews.append(review)

                except Exception as e:
                    print(f"    Error parsing review block: {e}")
                    self.error_tracker.record_attempt(ParseResult(
                        success=False,
                        warnings=[str(e)],
                        raw_block=block[:500] if 'block' in locals() else '',
                        strategy_used=match_info.get('pattern', 'unknown'),
                    ))
                    continue

        except Exception as e:
            print(f"    Error parsing reviews from text: {e}")

        return reviews

    def _clean_review_text(self, text: str) -> str:
        """Clean up review text by removing metadata."""
        # Remove common metadata patterns
        patterns_to_remove = [
            r"_\d+_/5",
            r"Visited:\s*\w+\s+\d{4}",
            r"Reviewed:\s*\w+\s+\d+,?\s*\d{4}",
            r"Experience level:\s*\w+",
            r"\[Full Review\]",
            r"^\s*[A-Z]{2}\s*$",  # Country codes on their own line
        ]

        for pattern in patterns_to_remove:
            text = re.sub(pattern, "", text, flags=re.IGNORECASE | re.MULTILINE)

        # Clean up whitespace
        text = re.sub(r"\s+", " ", text).strip()

        return text

    def _extract_country(self, location: str) -> str:
        """Extract country from location string."""
        if not location:
            return ""

        location = location.strip().upper()

        # Check if it's a country code
        if location in COUNTRY_CODES:
            return COUNTRY_CODES[location]

        # If contains comma, take the last part
        if "," in location:
            parts = [p.strip() for p in location.split(",")]
            last_part = parts[-1].upper()
            return COUNTRY_CODES.get(last_part, parts[-1])

        return location

    def _normalize_trip_type(self, trip_text: str) -> str:
        """Normalize trip type to standard categories."""
        trip_text = trip_text.lower()

        if any(w in trip_text for w in ["solo", "alone", "single"]):
            return "solo"
        elif any(w in trip_text for w in ["couple", "honeymoon", "romantic"]):
            return "couple"
        elif any(w in trip_text for w in ["family", "kids", "children"]):
            return "family"
        elif any(w in trip_text for w in ["friend", "friends"]):
            return "friends"
        elif any(w in trip_text for w in ["group", "tour"]):
            return "group"

        return trip_text

    def get_parsing_report(self) -> dict:
        """Get detailed parsing statistics and error report."""
        return self.error_tracker.get_report()

    def print_parsing_summary(self):
        """Print a summary of parsing results."""
        print(self.error_tracker.get_summary())

    async def scrape_all(
        self,
        max_operators: int = 50,
        max_reviews_per_operator: int = 50,
        resume: bool = True,
        max_operator_pages: int = 20,
    ) -> list[Review]:
        """Scrape reviews from multiple operators with enhanced tracking."""
        all_reviews = []
        processed_urls = set()

        # Reset error tracker for this run
        self.error_tracker.reset()

        if resume:
            progress = self.load_progress()
            if progress:
                processed_urls = set(progress.get("processed_urls", []))
                print(f"Resuming from {len(processed_urls)} previously processed operators")

        try:
            print("Fetching operator URLs...")
            operator_urls = await self.get_operator_urls(max_pages=max_operator_pages)
            print(f"Found {len(operator_urls)} operators")

            for i, url in enumerate(operator_urls[:max_operators]):
                if self._stop_requested:
                    break

                if url in processed_urls:
                    continue

                print(f"[{i+1}/{min(len(operator_urls), max_operators)}] Scraping: {url}")

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
                })

                # Adaptive rate limiting - slower after many requests
                if i > 50:
                    await asyncio.sleep(self.max_delay * 1.5)
                else:
                    await self.random_delay()

                # Print parsing stats every 10 operators
                if (i + 1) % 10 == 0:
                    report = self.get_parsing_report()
                    print(f"  [Parsing stats: {report['stats']['successful']} OK, "
                          f"{report['stats']['failed']} failed, "
                          f"{report['stats']['low_confidence']} low confidence]")

        finally:
            await self.stop()

        # Print final parsing summary
        print("\n=== Parsing Summary ===")
        self.print_parsing_summary()

        return all_reviews

    async def scrape_all_batched(
        self,
        max_operators: int = 100,
        max_reviews_per_operator: int = 1000,
        batch_callback=None,
        resume: bool = True,
    ) -> int:
        """
        Scrape with batched processing for large-scale operations.

        Args:
            max_operators: Maximum number of operators to scrape
            max_reviews_per_operator: Maximum reviews per operator
            batch_callback: Optional callback(reviews: list[Review]) called after each operator
            resume: Whether to resume from saved progress

        Returns:
            Total number of reviews scraped
        """
        total_reviews = 0
        processed_urls = set()

        self.error_tracker.reset()

        if resume:
            progress = self.load_progress()
            if progress:
                processed_urls = set(progress.get("processed_urls", []))
                total_reviews = progress.get("total_reviews", 0)
                print(f"Resuming: {len(processed_urls)} operators, {total_reviews} reviews")

        try:
            print("Fetching operator URLs (up to 20 pages)...")
            operator_urls = await self.get_operator_urls(max_pages=20)
            print(f"Found {len(operator_urls)} operators")

            for i, url in enumerate(operator_urls[:max_operators]):
                if self._stop_requested:
                    break

                if url in processed_urls:
                    continue

                print(f"[{i+1}/{min(len(operator_urls), max_operators)}] Scraping: {url}")

                try:
                    reviews = await self.scrape_reviews(url, max_reviews=max_reviews_per_operator)

                    if batch_callback and reviews:
                        batch_callback(reviews)

                    total_reviews += len(reviews)
                    print(f"  Found {len(reviews)} reviews (total: {total_reviews})")

                except Exception as e:
                    print(f"  Error: {e}")

                processed_urls.add(url)

                # Save progress frequently
                self.save_progress({
                    "processed_urls": list(processed_urls),
                    "total_reviews": total_reviews,
                })

                # Adaptive delay
                if i > 50:
                    await asyncio.sleep(self.max_delay * 2)
                else:
                    await self.random_delay()

        finally:
            await self.stop()

        print(f"\n=== Completed: {total_reviews} total reviews ===")
        self.print_parsing_summary()

        return total_reviews
