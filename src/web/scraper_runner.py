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
    parallel_workers: int = 4  # Number of parallel browser contexts for scraping


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

            # Load existing operator stats from database to skip fully-scraped operators
            operator_stats = db.get_all_operator_stats()
            self._sync_broadcast({
                "type": "checking_database",
                "message": f"Checking {len(operator_stats)} operators in database..."
            })

            # Filter operators:
            # 1. Not in processed_urls (session checkpoint)
            # 2. Not fully scraped (less than max_reviews in database)
            new_operator_urls = []
            skipped_full = 0
            for url in all_operator_urls:
                if url in processed_urls:
                    continue
                # Extract operator name from URL to check database
                op_name = url.split("/")[-1] if "/" in url else url
                existing_count = operator_stats.get(op_name, 0)
                if existing_count >= config.max_reviews_per_operator:
                    skipped_full += 1
                    continue
                new_operator_urls.append(url)

            # Limit to requested number of NEW operators
            # config.max_operators is the user's requested NEW operator count
            operator_urls = new_operator_urls[:config.max_operators]
            self.status.total_operators = len(operator_urls)

            self._sync_broadcast({
                "type": "operators_discovered",
                "total": len(all_operator_urls),
                "already_done": len(processed_urls),
                "skipped_full": skipped_full,
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

            # Scrape operators in parallel using worker pool
            self._sync_broadcast({
                "type": "parallel_scraping",
                "message": f"Scraping with {config.parallel_workers} parallel workers...",
                "workers": config.parallel_workers,
            })

            # Process operators in parallel batches
            completed_count = 0
            semaphore = asyncio.Semaphore(config.parallel_workers)

            async def scrape_worker(url: str, index: int):
                """Worker function to scrape a single operator."""
                nonlocal total_reviews, completed_count

                async with semaphore:
                    if self.status.should_stop:
                        return None

                    operator_name = url.split("/")[-1] if "/" in url else url

                    # Get existing review URLs for this operator to skip duplicates
                    existing_urls = db.get_operator_review_urls(operator_name)
                    existing_count = len(existing_urls)

                    self._sync_broadcast({
                        "type": "operator_started",
                        "index": index,
                        "total": self.status.total_operators,
                        "url": url,
                        "name": operator_name,
                        "existing_reviews": existing_count,
                    })

                    try:
                        # Create isolated context for this worker
                        context, page = await self._scraper.create_context()

                        try:
                            reviews = await self._scraper.scrape_reviews_with_page(
                                url, page, config.max_reviews_per_operator,
                                existing_urls=existing_urls  # Pass existing URLs to skip duplicates
                            )

                            # Save reviews to database and count new ones
                            new_reviews = 0
                            for review in reviews:
                                if review.url not in existing_urls:
                                    db.insert_review(review)
                                    new_reviews += 1

                            completed_count += 1
                            total_reviews += new_reviews  # Only count NEW reviews
                            self.status.total_reviews = total_reviews
                            self.status.current_operator_index = completed_count

                            self._sync_broadcast({
                                "type": "operator_completed",
                                "index": index,
                                "url": url,
                                "reviews_scraped": len(reviews),
                                "new_reviews": new_reviews,
                                "duplicates_skipped": len(reviews) - new_reviews,
                                "total_reviews": total_reviews,
                            })

                            return (url, len(reviews), None)

                        finally:
                            await context.close()

                    except Exception as e:
                        error_msg = str(e)
                        self.status.errors.append(error_msg)

                        self._sync_broadcast({
                            "type": "operator_error",
                            "index": index,
                            "url": url,
                            "error": error_msg,
                        })

                        if "captcha" in error_msg.lower():
                            self._sync_broadcast({
                                "type": "captcha_detected",
                                "message": "CAPTCHA detected. Please solve it manually.",
                                "requires_action": True,
                            })

                        return (url, 0, error_msg)

            # Launch all workers
            tasks = [
                scrape_worker(url, i + 1)
                for i, url in enumerate(operator_urls)
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Process results and save progress
            for result in results:
                if result and isinstance(result, tuple):
                    url, review_count, error = result
                    processed_urls.add(url)

            # Save final progress
            self._scraper.save_progress({
                "processed_urls": list(processed_urls),
                "total_reviews": total_reviews,
            })

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

    async def pause_scrape(self) -> bool:
        """Pause the scraper, saving detailed checkpoint for resume."""
        if not self.status.is_running:
            return False

        self.status.should_stop = True

        if self._scraper:
            self._scraper.request_pause()  # Sets both pause and stop flags

        await self.broadcast_event({
            "type": "pausing",
            "message": "Saving checkpoint...",
        })

        # Wait for thread to finish (it will save checkpoint)
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=30)

        # Get the saved checkpoint
        checkpoint = None
        if self._scraper:
            checkpoint = self._scraper.load_progress()

        # Update run as paused
        if self.status.run_id:
            db = Database()
            db.update_scrape_run(
                self.status.run_id,
                status='paused',
                reviews_collected=self.status.total_reviews,
                operators_completed=self.status.current_operator_index,
                errors=self.status.errors[-10:]
            )

        self.status.is_running = False
        sleep_manager.stop()

        await self.broadcast_event({
            "type": "paused",
            "message": "Scrape paused. You can safely disconnect.",
            "total_reviews": self.status.total_reviews,
            "operators_completed": self.status.current_operator_index,
            "checkpoint": checkpoint,
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
