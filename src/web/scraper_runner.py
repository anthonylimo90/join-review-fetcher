"""Background scraper execution with progress callbacks."""
import asyncio
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Callable, Any

from ..database.connection import Database
from ..scrapers.safaribookings import SafaribookingsScraper
from .sleep_manager import sleep_manager
from .websocket import manager as ws_manager


@dataclass
class ScrapeConfig:
    """Configuration for a scrape job."""
    source: str = "safaribookings"
    max_operators: int = 50
    max_reviews_per_operator: int = 200
    headless: bool = True
    resume: bool = True


@dataclass
class ScrapeStatus:
    """Current status of scrape job."""
    is_running: bool = False
    should_stop: bool = False
    started_at: Optional[datetime] = None
    current_operator: str = ""
    current_operator_index: int = 0
    total_operators: int = 0
    total_reviews: int = 0
    current_page: int = 0
    reviews_on_current_operator: int = 0
    parsing_stats: dict = field(default_factory=dict)
    errors: list = field(default_factory=list)
    config: Optional[ScrapeConfig] = None
    run_id: Optional[int] = None  # Database run ID for tracking


class ScraperRunner:
    """Run scraper in background with progress callbacks."""

    def __init__(self):
        self.status = ScrapeStatus()
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._scraper: Optional[SafaribookingsScraper] = None

    async def broadcast_event(self, event: dict):
        """Broadcast event to all WebSocket clients."""
        event["timestamp"] = datetime.now().isoformat()
        await ws_manager.broadcast(event)

    def _sync_broadcast(self, event: dict):
        """Synchronously broadcast event (for use in callbacks)."""
        event["timestamp"] = datetime.now().isoformat()
        try:
            if self._loop and self._loop.is_running():
                future = asyncio.run_coroutine_threadsafe(
                    ws_manager.broadcast(event),
                    self._loop
                )
                # Wait briefly to ensure message is sent
                try:
                    future.result(timeout=1.0)
                except Exception:
                    pass
        except Exception as e:
            print(f"Broadcast error: {e}")

    async def start_scrape(self, config: ScrapeConfig) -> bool:
        """Start a new scrape job."""
        if self.status.is_running:
            return False

        # Create run record in database
        db = Database()
        run_id = db.create_scrape_run(
            source=config.source,
            config={
                "max_operators": config.max_operators,
                "max_reviews_per_operator": config.max_reviews_per_operator,
                "headless": config.headless,
                "resume": config.resume,
            }
        )

        self.status = ScrapeStatus(
            is_running=True,
            started_at=datetime.now(),
            config=config,
            run_id=run_id,
        )

        # Start sleep prevention
        sleep_manager.start()

        # Store the event loop
        self._loop = asyncio.get_event_loop()

        # Broadcast start event
        await self.broadcast_event({
            "type": "started",
            "config": {
                "source": config.source,
                "max_operators": config.max_operators,
                "max_reviews_per_operator": config.max_reviews_per_operator,
                "headless": config.headless,
                "resume": config.resume,
            }
        })

        # Run scraper in background thread
        self._thread = threading.Thread(
            target=self._run_scraper,
            args=(config,),
            daemon=True
        )
        self._thread.start()

        return True

    def _run_scraper(self, config: ScrapeConfig):
        """Run the scraper (called in background thread)."""
        # Create new event loop for this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        db = Database()

        try:
            print(f"[ScraperRunner] Starting scrape with config: {config}")
            loop.run_until_complete(self._async_scrape(config))
            print("[ScraperRunner] Scrape completed successfully")

            # Update run as completed
            if self.status.run_id:
                db.update_scrape_run(
                    self.status.run_id,
                    status='completed',
                    reviews_collected=self.status.total_reviews,
                    operators_completed=self.status.current_operator_index,
                    errors=self.status.errors[-10:]
                )

            # Invalidate analytics cache so fresh data is shown
            try:
                from .routes import invalidate_analytics_cache
                invalidate_analytics_cache()
            except Exception:
                pass
        except Exception as e:
            import traceback
            error_msg = f"{str(e)}\n{traceback.format_exc()}"
            print(f"[ScraperRunner] Error: {error_msg}")
            self.status.errors.append(str(e))
            self._sync_broadcast({
                "type": "error",
                "message": str(e),
                "requires_action": False,
            })

            # Update run as failed
            if self.status.run_id:
                db.update_scrape_run(
                    self.status.run_id,
                    status='failed',
                    reviews_collected=self.status.total_reviews,
                    operators_completed=self.status.current_operator_index,
                    errors=self.status.errors[-10:]
                )
        finally:
            loop.close()
            self.status.is_running = False
            sleep_manager.stop()
            print("[ScraperRunner] Scraper stopped, cleanup complete")

    async def _async_scrape(self, config: ScrapeConfig):
        """Async scraping logic with progress callbacks."""
        db = Database()

        if config.source == "safaribookings":
            self._scraper = SafaribookingsScraper(headless=config.headless)
        else:
            raise ValueError(f"Unknown source: {config.source}")

        processed_urls = set()
        total_reviews = 0

        if config.resume:
            progress = self._scraper.load_progress()
            if progress:
                processed_urls = set(progress.get("processed_urls", []))
                total_reviews = progress.get("total_reviews", 0)
                self._sync_broadcast({
                    "type": "resumed",
                    "processed_operators": len(processed_urls),
                    "total_reviews": total_reviews,
                })

        try:
            await self._scraper.start()

            # Get operator URLs
            self._sync_broadcast({
                "type": "discovering_operators",
                "message": "Fetching operator URLs..."
            })

            # Calculate pages needed based on total operators we need to find
            # (processed + requested new), with ~20 operators per page
            total_needed = len(processed_urls) + config.max_operators
            pages_needed = max(20, (total_needed // 15) + 5)

            self._sync_broadcast({
                "type": "discovering_operators",
                "message": f"Scanning up to {pages_needed} listing pages..."
            })

            all_operator_urls = await self._scraper.get_operator_urls(max_pages=pages_needed)

            # Filter out already processed operators to get NEW operators only
            new_operator_urls = [url for url in all_operator_urls if url not in processed_urls]

            # Limit to requested number of NEW operators
            # config.max_operators is the user's requested NEW operator count
            operator_urls = new_operator_urls[:config.max_operators]
            self.status.total_operators = len(operator_urls)

            self._sync_broadcast({
                "type": "operators_discovered",
                "total": len(all_operator_urls),
                "already_done": len(processed_urls),
                "new_available": len(new_operator_urls),
                "to_scrape": self.status.total_operators,
                "operator_urls": operator_urls,
            })

            # Update run with operators total
            if self.status.run_id:
                db.update_scrape_run(
                    self.status.run_id,
                    operators_total=self.status.total_operators
                )

            # Scrape each operator (operator_urls is already filtered to NEW operators only)
            for i, url in enumerate(operator_urls):
                if self.status.should_stop:
                    break

                self.status.current_operator_index = i + 1
                self.status.current_operator = url
                self.status.current_page = 1
                self.status.reviews_on_current_operator = 0

                # Get operator name from URL
                operator_name = url.split("/")[-1] if "/" in url else url

                self._sync_broadcast({
                    "type": "operator_started",
                    "index": i + 1,
                    "total": self.status.total_operators,
                    "url": url,
                    "name": operator_name,
                })

                try:
                    reviews = await self._scrape_operator_with_progress(
                        url, config.max_reviews_per_operator
                    )

                    # Save reviews to database
                    for review in reviews:
                        db.insert_review(review)

                    total_reviews += len(reviews)
                    self.status.total_reviews = total_reviews
                    self.status.reviews_on_current_operator = len(reviews)

                    # Get parsing stats
                    parsing_report = self._scraper.get_parsing_report()
                    self.status.parsing_stats = parsing_report.get("stats", {})

                    self._sync_broadcast({
                        "type": "operator_completed",
                        "index": i + 1,
                        "url": url,
                        "reviews_extracted": len(reviews),
                        "total_reviews": total_reviews,
                        "parsing_stats": self.status.parsing_stats,
                    })

                except Exception as e:
                    error_msg = str(e)
                    self.status.errors.append(error_msg)

                    self._sync_broadcast({
                        "type": "operator_error",
                        "index": i + 1,
                        "url": url,
                        "error": error_msg,
                    })

                    # Check if CAPTCHA
                    if "captcha" in error_msg.lower():
                        self._sync_broadcast({
                            "type": "captcha_detected",
                            "message": "CAPTCHA detected. Please solve it manually.",
                            "requires_action": True,
                        })

                processed_urls.add(url)

                # Track operators scraped for adaptive rate limiting
                self._scraper.operators_scraped = len(processed_urls)

                # Save progress
                self._scraper.save_progress({
                    "processed_urls": list(processed_urls),
                    "total_reviews": total_reviews,
                })

                # Browser restart every 50 operators to prevent memory leaks
                if (i + 1) % 50 == 0 and i + 1 < len(operator_urls):
                    self._sync_broadcast({
                        "type": "browser_restart",
                        "message": f"Restarting browser after {i + 1} operators to clear memory",
                    })
                    await self._scraper.restart_browser()

                # Use adaptive rate limiting (slower as we scrape more)
                await self._scraper.adaptive_delay()

            # Completed
            duration = (datetime.now() - self.status.started_at).total_seconds()
            self._sync_broadcast({
                "type": "completed",
                "total_reviews": total_reviews,
                "operators_scraped": len(processed_urls),
                "duration_seconds": duration,
                "parsing_stats": self.status.parsing_stats,
            })

        except Exception as e:
            self._sync_broadcast({
                "type": "error",
                "message": str(e),
                "requires_action": False,
            })
        finally:
            await self._scraper.stop()

    async def _scrape_operator_with_progress(self, url: str, max_reviews: int) -> list:
        """Scrape operator with page-level progress updates."""
        # This wraps the scraper's scrape_reviews but adds page progress
        # For now, use the existing method directly
        # Future enhancement: modify scraper to accept progress callback

        reviews = await self._scraper.scrape_reviews(url, max_reviews=max_reviews)
        return reviews

    async def stop_scrape(self) -> bool:
        """Request scraper to stop."""
        if not self.status.is_running:
            return False

        self.status.should_stop = True

        if self._scraper:
            self._scraper.request_stop()

        await self.broadcast_event({
            "type": "stopping",
            "message": "Stop requested, finishing current operation...",
        })

        # Wait for thread to finish
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=30)

        # Update run as stopped
        if self.status.run_id:
            db = Database()
            db.update_scrape_run(
                self.status.run_id,
                status='stopped',
                reviews_collected=self.status.total_reviews,
                operators_completed=self.status.current_operator_index,
                errors=self.status.errors[-10:]
            )

        self.status.is_running = False
        sleep_manager.stop()

        await self.broadcast_event({
            "type": "stopped",
            "reason": "user_requested",
            "total_reviews": self.status.total_reviews,
        })

        return True

    def get_status(self) -> dict:
        """Get current scrape status."""
        return {
            "is_running": self.status.is_running,
            "started_at": self.status.started_at.isoformat() if self.status.started_at else None,
            "current_operator": self.status.current_operator,
            "current_operator_index": self.status.current_operator_index,
            "total_operators": self.status.total_operators,
            "total_reviews": self.status.total_reviews,
            "current_page": self.status.current_page,
            "reviews_on_current_operator": self.status.reviews_on_current_operator,
            "parsing_stats": self.status.parsing_stats,
            "errors": self.status.errors[-10:],  # Last 10 errors
            "sleep_prevented": sleep_manager.is_active,
            "config": {
                "source": self.status.config.source if self.status.config else None,
                "max_operators": self.status.config.max_operators if self.status.config else None,
                "max_reviews_per_operator": self.status.config.max_reviews_per_operator if self.status.config else None,
            } if self.status.config else None,
        }


# Global scraper runner instance
scraper_runner = ScraperRunner()
