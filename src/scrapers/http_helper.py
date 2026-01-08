"""Fast HTTP-based scraping helpers using httpx and lxml.

This module provides faster alternatives to browser-based scraping for
operations that don't require JavaScript rendering.
"""
import asyncio
import re
from typing import Optional

try:
    import httpx
    from lxml import html
    HTTP_AVAILABLE = True
except ImportError:
    HTTP_AVAILABLE = False


async def fetch_operator_urls_fast(
    base_url: str = "https://www.safaribookings.com",
    max_pages: int = 20,
    timeout: float = 30.0,
) -> list[str]:
    """
    Fetch operator URLs using HTTP instead of browser - 10-20x faster.

    This method fetches the operator listing pages directly via HTTP
    and parses them with lxml, avoiding browser overhead.

    Args:
        base_url: Base URL of the site
        max_pages: Maximum number of listing pages to fetch
        timeout: Request timeout in seconds

    Returns:
        List of operator URLs
    """
    if not HTTP_AVAILABLE:
        raise ImportError("httpx and lxml are required for fast HTTP fetching. Install with: pip install httpx lxml")

    urls = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }

    async with httpx.AsyncClient(timeout=timeout, headers=headers, follow_redirects=True) as client:
        for page_num in range(1, max_pages + 1):
            try:
                # SafariBookings operator listing URL
                page_url = f"{base_url}/operators" if page_num == 1 else f"{base_url}/operators?page={page_num}"

                response = await client.get(page_url)
                response.raise_for_status()

                # Parse HTML with lxml
                tree = html.fromstring(response.content)

                # Extract operator links - looking for li[data-id] a pattern
                links = tree.xpath('//li[@data-id]//a/@href')

                if not links:
                    # Try alternative pattern
                    links = tree.xpath('//a[contains(@href, "/p")]/@href')

                # Filter and normalize URLs
                for href in links:
                    if re.search(r"/p\d+", href):
                        if href.startswith("http"):
                            full_url = href
                        else:
                            full_url = f"{base_url}{href}" if href.startswith("/") else f"{base_url}/{href}"

                        if full_url not in urls and "safaribookings.com/p" in full_url:
                            urls.append(full_url)

                # If no links found, we've reached the end
                if not links:
                    break

                # Small delay between requests
                await asyncio.sleep(0.2)

            except httpx.HTTPStatusError as e:
                print(f"  HTTP error on page {page_num}: {e}")
                break
            except Exception as e:
                print(f"  Error fetching page {page_num}: {e}")
                break

    return urls


def is_http_available() -> bool:
    """Check if HTTP dependencies are available."""
    return HTTP_AVAILABLE
