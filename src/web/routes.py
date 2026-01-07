"""REST API routes for the scraper web UI."""
import json
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ..database.connection import Database
from .scraper_runner import scraper_runner, ScrapeConfig
from .sleep_manager import sleep_manager

router = APIRouter(prefix="/api")


class ScrapeStartRequest(BaseModel):
    """Request body for starting a scrape."""
    source: str = "safaribookings"
    max_operators: int = 50
    max_reviews_per_operator: int = 50
    headless: bool = True
    resume: bool = True


@router.get("/status")
async def get_status():
    """Get server and scraper status."""
    return {
        "server": "running",
        "scraper": scraper_runner.get_status(),
        "sleep_prevented": sleep_manager.is_active,
    }


@router.get("/stats")
async def get_stats():
    """Get database statistics."""
    db = Database()

    # Get review counts
    total_reviews = db.get_review_count()
    safaribookings_reviews = db.get_review_count("safaribookings")
    tripadvisor_reviews = db.get_review_count("tripadvisor")

    # Get additional stats from database
    import sqlite3
    stats = {
        "total_reviews": total_reviews,
        "by_source": {
            "safaribookings": safaribookings_reviews,
            "tripadvisor": tripadvisor_reviews,
        },
        "distinct_operators": 0,
        "countries_represented": 0,
        "avg_rating": 0,
        "reviews_with_guides": 0,
    }

    try:
        conn = sqlite3.connect(db.db_path)
        cursor = conn.cursor()

        # Distinct operators
        cursor.execute("SELECT COUNT(DISTINCT operator_name) FROM reviews")
        stats["distinct_operators"] = cursor.fetchone()[0] or 0

        # Distinct countries
        cursor.execute("SELECT COUNT(DISTINCT reviewer_country) FROM reviews WHERE reviewer_country IS NOT NULL AND reviewer_country != ''")
        stats["countries_represented"] = cursor.fetchone()[0] or 0

        # Average rating
        cursor.execute("SELECT AVG(rating) FROM reviews WHERE rating IS NOT NULL")
        avg = cursor.fetchone()[0]
        stats["avg_rating"] = round(avg, 2) if avg else 0

        # Reviews with guide mentions
        cursor.execute("SELECT COUNT(*) FROM reviews WHERE guide_names_mentioned IS NOT NULL AND guide_names_mentioned != '[]'")
        stats["reviews_with_guides"] = cursor.fetchone()[0] or 0

        # Rating distribution
        cursor.execute("""
            SELECT
                CASE
                    WHEN rating >= 4.5 THEN '4.5-5'
                    WHEN rating >= 4 THEN '4-4.5'
                    WHEN rating >= 3 THEN '3-4'
                    WHEN rating >= 2 THEN '2-3'
                    ELSE '0-2'
                END as range,
                COUNT(*) as count
            FROM reviews
            WHERE rating IS NOT NULL
            GROUP BY range
            ORDER BY range DESC
        """)
        stats["rating_distribution"] = {row[0]: row[1] for row in cursor.fetchall()}

        # Trip type distribution
        cursor.execute("""
            SELECT trip_type, COUNT(*) as count
            FROM reviews
            WHERE trip_type IS NOT NULL AND trip_type != ''
            GROUP BY trip_type
            ORDER BY count DESC
            LIMIT 10
        """)
        stats["trip_types"] = {row[0]: row[1] for row in cursor.fetchall()}

        # Top countries
        cursor.execute("""
            SELECT reviewer_country, COUNT(*) as count
            FROM reviews
            WHERE reviewer_country IS NOT NULL AND reviewer_country != ''
            GROUP BY reviewer_country
            ORDER BY count DESC
            LIMIT 10
        """)
        stats["top_countries"] = {row[0]: row[1] for row in cursor.fetchall()}

        conn.close()
    except Exception as e:
        print(f"Error getting stats: {e}")

    return stats


@router.get("/reviews")
async def get_reviews(
    source: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
):
    """Get reviews with pagination."""
    db = Database()

    import sqlite3
    conn = sqlite3.connect(db.db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    if source:
        cursor.execute(
            "SELECT * FROM reviews WHERE source = ? ORDER BY id DESC LIMIT ? OFFSET ?",
            (source, limit, offset)
        )
    else:
        cursor.execute(
            "SELECT * FROM reviews ORDER BY id DESC LIMIT ? OFFSET ?",
            (limit, offset)
        )

    reviews = [dict(row) for row in cursor.fetchall()]
    conn.close()

    return {
        "reviews": reviews,
        "limit": limit,
        "offset": offset,
        "count": len(reviews),
    }


@router.get("/progress")
async def get_progress():
    """Get scraper checkpoint progress."""
    progress_file = Path("data/scraper_state.json")

    if not progress_file.exists():
        return {"exists": False, "data": None}

    try:
        with open(progress_file) as f:
            data = json.load(f)
        return {"exists": True, "data": data}
    except Exception as e:
        return {"exists": False, "error": str(e)}


@router.post("/scrape/start")
async def start_scrape(request: ScrapeStartRequest):
    """Start a new scrape job."""
    if scraper_runner.status.is_running:
        raise HTTPException(status_code=400, detail="Scrape already running")

    config = ScrapeConfig(
        source=request.source,
        max_operators=request.max_operators,
        max_reviews_per_operator=request.max_reviews_per_operator,
        headless=request.headless,
        resume=request.resume,
    )

    success = await scraper_runner.start_scrape(config)

    if not success:
        raise HTTPException(status_code=500, detail="Failed to start scrape")

    return {
        "status": "started",
        "config": {
            "source": config.source,
            "max_operators": config.max_operators,
            "max_reviews_per_operator": config.max_reviews_per_operator,
            "headless": config.headless,
            "resume": config.resume,
        }
    }


@router.post("/scrape/stop")
async def stop_scrape():
    """Stop the current scrape job."""
    if not scraper_runner.status.is_running:
        raise HTTPException(status_code=400, detail="No scrape running")

    success = await scraper_runner.stop_scrape()

    if not success:
        raise HTTPException(status_code=500, detail="Failed to stop scrape")

    return {"status": "stopped"}


@router.post("/scrape/clear")
async def clear_progress():
    """Clear scraper progress checkpoint."""
    progress_file = Path("data/scraper_state.json")

    if progress_file.exists():
        progress_file.unlink()

    return {"status": "cleared"}


@router.get("/analysis/guides")
async def get_guide_analysis():
    """Get guide mention statistics."""
    db = Database()
    return db.get_guide_mention_stats()
