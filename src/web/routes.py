"""REST API routes for the scraper web UI."""
import json
import io
import csv
from pathlib import Path
from typing import Optional, Any
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from ..database.connection import Database
from .scraper_runner import scraper_runner, ScrapeConfig
from .sleep_manager import sleep_manager

router = APIRouter(prefix="/api")


# ==================== SIMPLE CACHE ====================

class SimpleCache:
    """Simple in-memory cache with TTL."""

    def __init__(self, default_ttl: int = 300):
        self._cache: dict[str, tuple[Any, datetime]] = {}
        self._default_ttl = default_ttl

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache if not expired."""
        if key in self._cache:
            value, expires_at = self._cache[key]
            if datetime.now() < expires_at:
                return value
            del self._cache[key]
        return None

    def set(self, key: str, value: Any, ttl: Optional[int] = None):
        """Set value in cache with TTL in seconds."""
        ttl = ttl or self._default_ttl
        expires_at = datetime.now() + timedelta(seconds=ttl)
        self._cache[key] = (value, expires_at)

    def invalidate(self, key: str):
        """Remove key from cache."""
        self._cache.pop(key, None)

    def invalidate_prefix(self, prefix: str):
        """Remove all keys starting with prefix."""
        keys_to_remove = [k for k in self._cache if k.startswith(prefix)]
        for key in keys_to_remove:
            del self._cache[key]


# Global cache instance (5 minute default TTL)
cache = SimpleCache(default_ttl=300)


def invalidate_analytics_cache():
    """Invalidate all analytics caches (call after scrape completes)."""
    cache.invalidate("stats")
    cache.invalidate("countries")
    cache.invalidate("analysis_guides")


class ScrapeStartRequest(BaseModel):
    """Request body for starting a scrape."""
    source: str = "safaribookings"
    max_operators: int = 50
    max_reviews_per_operator: int = 50
    headless: bool = True
    resume: bool = True


# ==================== STATUS ENDPOINTS ====================

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
    """Get database statistics (cached for 5 minutes)."""
    # Check cache first
    cached = cache.get("stats")
    if cached is not None:
        return cached

    db = Database()
    import sqlite3

    total_reviews = db.get_review_count()
    safaribookings_reviews = db.get_review_count("safaribookings")
    tripadvisor_reviews = db.get_review_count("tripadvisor")

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

        cursor.execute("SELECT COUNT(DISTINCT operator_name) FROM reviews")
        stats["distinct_operators"] = cursor.fetchone()[0] or 0

        cursor.execute("SELECT COUNT(DISTINCT reviewer_country) FROM reviews WHERE reviewer_country IS NOT NULL AND reviewer_country != ''")
        stats["countries_represented"] = cursor.fetchone()[0] or 0

        cursor.execute("SELECT AVG(rating) FROM reviews WHERE rating IS NOT NULL")
        avg = cursor.fetchone()[0]
        stats["avg_rating"] = round(avg, 2) if avg else 0

        cursor.execute("SELECT COUNT(*) FROM reviews WHERE guide_names_mentioned IS NOT NULL AND guide_names_mentioned != '[]'")
        stats["reviews_with_guides"] = cursor.fetchone()[0] or 0

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
            FROM reviews WHERE rating IS NOT NULL
            GROUP BY range ORDER BY range DESC
        """)
        stats["rating_distribution"] = {row[0]: row[1] for row in cursor.fetchall()}

        cursor.execute("""
            SELECT trip_type, COUNT(*) as count FROM reviews
            WHERE trip_type IS NOT NULL AND trip_type != ''
            GROUP BY trip_type ORDER BY count DESC LIMIT 10
        """)
        stats["trip_types"] = {row[0]: row[1] for row in cursor.fetchall()}

        cursor.execute("""
            SELECT reviewer_country, COUNT(*) as count FROM reviews
            WHERE reviewer_country IS NOT NULL AND reviewer_country != ''
            GROUP BY reviewer_country ORDER BY count DESC LIMIT 10
        """)
        stats["top_countries"] = {row[0]: row[1] for row in cursor.fetchall()}

        conn.close()
    except Exception as e:
        print(f"Error getting stats: {e}")

    # Cache for 5 minutes
    cache.set("stats", stats)
    return stats


# ==================== OPERATORS ENDPOINTS ====================

@router.get("/operators")
async def get_operators(
    search: Optional[str] = None,
    sort: str = "reviews",
    source: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
):
    """Get operators with review counts and stats."""
    db = Database()
    import sqlite3

    conn = sqlite3.connect(db.db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Build query
    where_clauses = ["operator_name IS NOT NULL AND operator_name != ''"]
    params = []

    if search:
        where_clauses.append("operator_name LIKE ?")
        params.append(f"%{search}%")

    if source:
        where_clauses.append("source = ?")
        params.append(source)

    where_sql = " AND ".join(where_clauses)

    # Sort order
    order_map = {
        "reviews": "review_count DESC",
        "rating": "avg_rating DESC",
        "name": "operator_name ASC",
    }
    order_sql = order_map.get(sort, "review_count DESC")

    # Get total count
    cursor.execute(f"""
        SELECT COUNT(DISTINCT operator_name) FROM reviews WHERE {where_sql}
    """, params)
    total = cursor.fetchone()[0] or 0

    # Get operators with stats
    cursor.execute(f"""
        SELECT
            operator_name,
            COUNT(*) as review_count,
            AVG(rating) as avg_rating,
            source,
            MIN(scraped_at) as first_scraped,
            MAX(scraped_at) as last_scraped
        FROM reviews
        WHERE {where_sql}
        GROUP BY operator_name
        ORDER BY {order_sql}
        LIMIT ? OFFSET ?
    """, params + [limit, offset])

    operators = [dict(row) for row in cursor.fetchall()]
    conn.close()

    return {
        "operators": operators,
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@router.get("/operators/{operator_name}")
async def get_operator_detail(operator_name: str):
    """Get details for a specific operator."""
    db = Database()
    import sqlite3

    conn = sqlite3.connect(db.db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            operator_name,
            COUNT(*) as review_count,
            AVG(rating) as avg_rating,
            source
        FROM reviews
        WHERE operator_name = ?
        GROUP BY operator_name
    """, (operator_name,))

    row = cursor.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Operator not found")

    operator = dict(row)

    # Get recent reviews - select only needed columns
    cursor.execute("""
        SELECT id, reviewer_name, reviewer_country, rating, title, text,
               travel_date, review_date, trip_type
        FROM reviews WHERE operator_name = ?
        ORDER BY review_date DESC LIMIT 10
    """, (operator_name,))
    operator["recent_reviews"] = [dict(r) for r in cursor.fetchall()]

    conn.close()
    return operator


# ==================== REVIEWS ENDPOINTS ====================

@router.get("/reviews")
async def get_reviews(
    search: Optional[str] = None,
    operator: Optional[str] = None,
    country: Optional[str] = None,
    source: Optional[str] = None,
    rating_min: Optional[float] = None,
    rating_max: Optional[float] = None,
    limit: int = 20,
    offset: int = 0,
):
    """Get reviews with filtering and pagination."""
    db = Database()
    import sqlite3

    conn = sqlite3.connect(db.db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Build query
    where_clauses = ["1=1"]
    params = []

    if search:
        where_clauses.append("(text LIKE ? OR title LIKE ? OR reviewer_name LIKE ?)")
        params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])

    if operator:
        where_clauses.append("operator_name = ?")
        params.append(operator)

    if country:
        where_clauses.append("reviewer_country = ?")
        params.append(country)

    if source:
        where_clauses.append("source = ?")
        params.append(source)

    if rating_min is not None:
        where_clauses.append("rating >= ?")
        params.append(rating_min)

    if rating_max is not None:
        where_clauses.append("rating <= ?")
        params.append(rating_max)

    where_sql = " AND ".join(where_clauses)

    # Get total count
    cursor.execute(f"SELECT COUNT(*) FROM reviews WHERE {where_sql}", params)
    total = cursor.fetchone()[0] or 0

    # Get reviews - select only columns needed for list view
    cursor.execute(f"""
        SELECT id, source, operator_name, reviewer_name, reviewer_location,
               reviewer_country, rating, title, text, travel_date,
               review_date, trip_type, scraped_at
        FROM reviews WHERE {where_sql}
        ORDER BY id DESC LIMIT ? OFFSET ?
    """, params + [limit, offset])

    reviews = [dict(row) for row in cursor.fetchall()]
    conn.close()

    return {
        "reviews": reviews,
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@router.get("/countries")
async def get_countries():
    """Get list of reviewer countries (cached for 5 minutes)."""
    # Check cache first
    cached = cache.get("countries")
    if cached is not None:
        return cached

    db = Database()
    import sqlite3

    conn = sqlite3.connect(db.db_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT DISTINCT reviewer_country FROM reviews
        WHERE reviewer_country IS NOT NULL AND reviewer_country != ''
        ORDER BY reviewer_country
    """)

    countries = [row[0] for row in cursor.fetchall()]
    conn.close()

    result = {"countries": countries}
    cache.set("countries", result)
    return result


# ==================== SCRAPE CONTROL ENDPOINTS ====================

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


# ==================== SCRAPE PREVIEW ENDPOINT ====================

@router.get("/scrape/preview")
async def preview_scrape(
    source: str = "safaribookings",
    max_operators: int = 50,
    resume: bool = True,
):
    """Preview what a scrape would do - how many new vs skipped operators."""
    progress_file = Path("data/scraper_state.json")

    processed_urls = []
    checkpoint_reviews = 0

    if resume and progress_file.exists():
        try:
            with open(progress_file) as f:
                data = json.load(f)
            source_data = data.get(source, {})
            processed_urls = source_data.get("processed_urls", [])
            checkpoint_reviews = source_data.get("total_reviews", 0)
        except Exception:
            pass

    # Get total operators in database
    db = Database()
    import sqlite3
    conn = sqlite3.connect(db.db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(DISTINCT operator_name) FROM reviews WHERE source = ?", (source,))
    db_operators = cursor.fetchone()[0] or 0
    cursor.execute("SELECT COUNT(*) FROM reviews WHERE source = ?", (source,))
    db_reviews = cursor.fetchone()[0] or 0
    conn.close()

    checkpoint_operators = len(processed_urls)

    # Estimate total operators available (SafariBookings has 3000+ tour operators)
    estimated_total_available = 3500 if source == "safaribookings" else 500
    remaining_available = max(0, estimated_total_available - checkpoint_operators)

    # Simple logic: user enters how many NEW operators they want
    if resume:
        # User wants max_operators NEW operators, we skip already processed
        new_operators = min(max_operators, remaining_available)
        operators_to_skip = checkpoint_operators
        # Actual limit to send to scraper = checkpoint + requested new
        effective_max = checkpoint_operators + max_operators
    else:
        # Without resume, start fresh - may re-scrape same operators
        new_operators = max_operators
        operators_to_skip = 0
        effective_max = max_operators

    will_get_new_data = new_operators > 0

    return {
        "source": source,
        "max_operators": max_operators,
        "effective_max_operators": effective_max,  # What to actually send to scraper
        "resume": resume,
        "checkpoint": {
            "operators_processed": checkpoint_operators,
            "reviews_collected": checkpoint_reviews,
        },
        "database": {
            "operators": db_operators,
            "reviews": db_reviews,
        },
        "preview": {
            "new_operators": new_operators,
            "operators_to_skip": operators_to_skip,
            "remaining_available": remaining_available,
            "estimated_total_available": estimated_total_available,
            "will_get_new_data": will_get_new_data,
        },
        "time_estimate": _calculate_time_estimate(new_operators),
        "recommendation": {
            "message": _get_preview_message_simple(new_operators, remaining_available, resume, checkpoint_operators),
        }
    }


def _calculate_time_estimate(num_operators: int) -> dict:
    """Calculate estimated scrape time based on historical data."""
    # Based on historical runs: ~0.55 minutes per operator average
    minutes_per_operator = 0.55
    total_minutes = num_operators * minutes_per_operator

    hours = int(total_minutes // 60)
    minutes = int(total_minutes % 60)

    if hours > 0:
        time_str = f"{hours}h {minutes}m"
    elif minutes > 0:
        time_str = f"{minutes}m"
    else:
        time_str = "< 1m"

    return {
        "minutes": round(total_minutes, 1),
        "formatted": time_str,
        "per_operator_mins": minutes_per_operator,
    }


def _get_preview_message_simple(new_operators: int, remaining: int, resume: bool, checkpoint: int) -> str:
    """Generate a helpful message about the scrape preview."""
    if not resume:
        return "Resume disabled - will start from beginning (duplicate reviews will be skipped)"

    if checkpoint == 0:
        return f"Fresh start - will scrape {new_operators} operators"

    if remaining == 0:
        return "All available operators have been scraped!"

    if new_operators == 0:
        return "Enter number of new operators to scrape"

    return f"Will scrape {new_operators} new operators (skipping {checkpoint} already done)"


# ==================== RUN HISTORY ENDPOINTS ====================

@router.get("/runs")
async def get_runs(limit: int = 20):
    """Get scrape run history."""
    db = Database()
    runs = db.get_scrape_runs(limit)
    return {"runs": runs}


@router.get("/runs/{run_id}")
async def get_run(run_id: int):
    """Get details of a specific run."""
    db = Database()
    run = db.get_scrape_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return run


# ==================== ANALYSIS ENDPOINTS ====================

@router.get("/analysis/guides")
async def get_guide_analysis():
    """Get guide mention statistics (cached for 5 minutes)."""
    # Check cache first
    cached = cache.get("analysis_guides")
    if cached is not None:
        return cached

    db = Database()
    result = db.get_guide_mention_stats()
    cache.set("analysis_guides", result)
    return result


@router.get("/analysis/guide-intelligence")
async def get_guide_intelligence():
    """Get comprehensive guide intelligence analysis (cached for 10 minutes)."""
    # Check cache first (longer TTL as this is expensive to compute)
    cached = cache.get("guide_intelligence")
    if cached is not None:
        return cached

    db = Database()
    result = db.get_guide_intelligence()
    cache.set("guide_intelligence", result, ttl=600)  # 10 minute cache
    return result


# ==================== EXPORT ENDPOINTS ====================

@router.get("/export/csv")
async def export_csv(
    reviews: bool = True,
    guide_analysis: bool = False,
    demographics: bool = False,
    decision_factors: bool = False,
):
    """Export data as CSV."""
    db = Database()
    import sqlite3

    output = io.StringIO()
    conn = sqlite3.connect(db.db_path)
    conn.row_factory = sqlite3.Row

    if reviews:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM reviews")
        rows = cursor.fetchall()

        if rows:
            writer = csv.writer(output)
            writer.writerow(rows[0].keys())
            for row in rows:
                writer.writerow(list(row))

    conn.close()
    output.seek(0)

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=safari_reviews_{datetime.now().strftime('%Y%m%d')}.csv"}
    )


@router.get("/export/json")
async def export_json(
    reviews: bool = True,
    guide_analysis: bool = False,
    demographics: bool = False,
    decision_factors: bool = False,
):
    """Export data as JSON."""
    db = Database()
    import sqlite3

    conn = sqlite3.connect(db.db_path)
    conn.row_factory = sqlite3.Row

    data = {}

    if reviews:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM reviews")
        data["reviews"] = [dict(row) for row in cursor.fetchall()]

    if guide_analysis:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM guide_analysis")
        data["guide_analysis"] = [dict(row) for row in cursor.fetchall()]

    if demographics:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM demographics")
        data["demographics"] = [dict(row) for row in cursor.fetchall()]

    if decision_factors:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM decision_factors")
        data["decision_factors"] = [dict(row) for row in cursor.fetchall()]

    conn.close()

    output = json.dumps(data, indent=2)

    return StreamingResponse(
        iter([output]),
        media_type="application/json",
        headers={"Content-Disposition": f"attachment; filename=safari_reviews_{datetime.now().strftime('%Y%m%d')}.json"}
    )
