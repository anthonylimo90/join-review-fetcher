"""Safaribookings.com scraper - Updated with actual website selectors."""
import asyncio
import re
from typing import Optional
from urllib.parse import urljoin

from playwright.async_api import Page, ElementHandle

from .base import BaseScraper
from ..database.models import Review


# Country code to full name mapping
COUNTRY_CODES = {
    "US": "United States", "UK": "United Kingdom", "GB": "United Kingdom",
    "CA": "Canada", "AU": "Australia", "DE": "Germany", "FR": "France",
    "IT": "Italy", "ES": "Spain", "NL": "Netherlands", "BE": "Belgium",
    "CH": "Switzerland", "AT": "Austria", "SE": "Sweden", "NO": "Norway",
    "DK": "Denmark", "FI": "Finland", "IE": "Ireland", "NZ": "New Zealand",
    "ZA": "South Africa", "KE": "Kenya", "TZ": "Tanzania", "IN": "India",
    "SG": "Singapore", "JP": "Japan", "CN": "China", "BR": "Brazil",
    "MX": "Mexico", "AR": "Argentina", "PL": "Poland", "CZ": "Czech Republic",
    "PT": "Portugal", "GR": "Greece", "HU": "Hungary", "RO": "Romania",
}


class SafaribookingsScraper(BaseScraper):
    """Scraper for Safaribookings.com safari reviews."""

    BASE_URL = "https://www.safaribookings.com"

    @property
    def name(self) -> str:
        return "safaribookings"

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

    async def _dismiss_cookie_popup(self):
        """Dismiss cookie consent popups."""
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
                    btn = await self.page.query_selector(selector)
                    if btn:
                        await btn.click()
                        await asyncio.sleep(1)
                        print("  Dismissed cookie popup")
                        return
                except Exception:
                    continue

        except Exception:
            pass

    async def get_operator_urls(self, max_pages: int = 10) -> list[str]:
        """Get list of safari operator URLs from Safaribookings."""
        if not self.page:
            await self.start()

        operators = []
        base_url = f"{self.BASE_URL}/operators"

        print(f"Loading operators page: {base_url}", flush=True)
        await self.page.goto(base_url, wait_until="domcontentloaded")
        await asyncio.sleep(2)  # Wait for page to fully load

        # Dismiss cookie popup first
        await self._dismiss_cookie_popup()
        await self.random_delay()

        if await self.check_for_captcha():
            await self.handle_captcha()

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
                    await self.random_delay()
                    page_num += 1

                    if await self.check_for_captcha():
                        await self.handle_captcha()
                except Exception as e:
                    print(f"  Pagination error: {e}")
                    break
            else:
                break

            if page_num % 3 == 0:
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
        try:
            await self.page.goto(reviews_url, wait_until="domcontentloaded")
            await asyncio.sleep(2)  # Wait for content to load
        except Exception as e:
            print(f"  Error loading {reviews_url}: {e}")
            return reviews

        print("  Page loaded, dismissing cookies...")
        # Dismiss cookie popup first
        await self._dismiss_cookie_popup()

        print("  Checking for CAPTCHA...")
        if await self.check_for_captcha():
            await self.handle_captcha()

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
                    await self.random_delay()
                    page_num += 1
                except Exception as e:
                    print(f"  Pagination error: {e}")
                    break
            else:
                print("  No more pages or max reviews reached")
                break

            if page_num % 2 == 0:
                self.save_progress({
                    "current_url": operator_url,
                    "reviews_count": len(reviews),
                    "page": page_num,
                })

        print(f"  Total reviews extracted: {len(reviews)}")
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
        """Parse reviews from page text using Safaribookings' actual structure."""
        reviews = []

        try:
            body = await self.page.query_selector("body")
            if not body:
                print("    Could not find body element")
                return reviews

            full_text = await body.inner_text()
            print(f"    Got page text: {len(full_text)} characters")

            # Safaribookings format:
            # "Name   –    CC Visited: Month Year Reviewed: Date"
            # Then: "Email Name  |  age  |  Experience level: X"
            # Then: "Title"
            # Then: " rating/5"
            # Then: "Review text"

            # Split by the reviewer pattern: "Name – CC Visited:"
            # Name is typically "First Last" or "First L." format
            review_pattern = r"\n([A-Z][a-z]+(?:\s+[A-Z][a-z\.]+)?)\s+–\s+([A-Z]{2})\s+Visited:\s*(\w+\s+\d{4})\s+Reviewed:\s*([A-Za-z]+\s+\d+,?\s*\d{4})"

            matches = list(re.finditer(review_pattern, full_text))
            print(f"    Regex found {len(matches)} reviewer matches")

            for i, match in enumerate(matches):
                try:
                    # Create unique URL for each review using reviewer name and position
                    reviewer_name = match.group(1).strip()
                    review_url = f"{operator_url}#review-{i+1}-{reviewer_name.replace(' ', '-').lower()}"

                    review = Review(
                        source="safaribookings",
                        url=review_url,
                        operator_name=operator_name,
                    )

                    # Extract from regex groups
                    review.reviewer_name = match.group(1).strip()
                    country_code = match.group(2)
                    review.reviewer_country = COUNTRY_CODES.get(country_code, country_code)
                    review.travel_date = match.group(3)
                    review.review_date = match.group(4)

                    # Get text between this match and next match (or end)
                    start_pos = match.end()
                    end_pos = matches[i + 1].start() if i + 1 < len(matches) else len(full_text)
                    block = full_text[start_pos:end_pos]

                    # Extract rating - " X/5" pattern
                    rating_match = re.search(r"\s(\d+(?:\.\d+)?)\s*/\s*5", block)
                    if rating_match:
                        review.rating = float(rating_match.group(1))

                    # Extract experience level
                    exp_match = re.search(r"Experience level:\s*([^\n|]+)", block)
                    if exp_match:
                        exp = exp_match.group(1).strip().lower()
                        if "first" in exp:
                            review.trip_type = "first_safari"
                        elif "2-5" in exp or "repeat" in exp:
                            review.trip_type = "repeat"

                    # Extract age range
                    age_match = re.search(r"(\d+-\d+)\s*years", block)

                    # Extract title and text
                    # Block format after header:
                    # Email Name | age | Experience level: X
                    # Title text
                    #  X/5
                    # Review text
                    # Was this review helpful? Yes No Link to This Review

                    lines = block.strip().split("\n")

                    # Find the rating line position
                    rating_line_idx = -1
                    for idx, line in enumerate(lines):
                        if re.match(r"^\s*\d+\s*/\s*5\s*$", line.strip()):
                            rating_line_idx = idx
                            break

                    # Title is usually the line just before rating
                    if rating_line_idx > 0:
                        # Look for title - skip metadata lines
                        for idx in range(rating_line_idx - 1, -1, -1):
                            line = lines[idx].strip()
                            if line and not any(x in line.lower() for x in [
                                "email", "experience level", "years of age", "|"
                            ]):
                                review.title = line
                                break

                    # Text is everything after rating until the "Was this review helpful?" line
                    text_lines = []
                    if rating_line_idx >= 0:
                        for idx in range(rating_line_idx + 1, len(lines)):
                            line = lines[idx].strip()

                            # Stop at the feedback section
                            if "was this review helpful" in line.lower():
                                break
                            if line.lower() in ["yes", "no"] or "link to this review" in line.lower():
                                break

                            # Skip very short lines
                            if len(line) > 10:
                                text_lines.append(line)

                    review.text = " ".join(text_lines)

                    if review.text and len(review.text) > 30:
                        reviews.append(review)

                except Exception as e:
                    print(f"Error parsing review block: {e}")
                    continue

        except Exception as e:
            print(f"Error parsing reviews from text: {e}")

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

    async def scrape_all(
        self,
        max_operators: int = 50,
        max_reviews_per_operator: int = 50,
        resume: bool = True,
    ) -> list[Review]:
        """Scrape reviews from multiple operators."""
        all_reviews = []
        processed_urls = set()

        if resume:
            progress = self.load_progress()
            if progress:
                processed_urls = set(progress.get("processed_urls", []))
                print(f"Resuming from {len(processed_urls)} previously processed operators")

        try:
            print("Fetching operator URLs...")
            operator_urls = await self.get_operator_urls(max_pages=5)
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

                await self.random_delay()

        finally:
            await self.stop()

        return all_reviews
