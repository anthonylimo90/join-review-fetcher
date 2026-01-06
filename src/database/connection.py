"""Database connection and operations."""
import sqlite3
from pathlib import Path
from typing import Optional
from contextlib import contextmanager

from .models import Review, GuideAnalysis, DecisionFactor, Demographic


class Database:
    """SQLite database manager for safari reviews."""

    def __init__(self, db_path: str = "data/reviews.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @contextmanager
    def _get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def _init_db(self):
        """Initialize database schema."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Reviews table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS reviews (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    url TEXT UNIQUE NOT NULL,
                    operator_name TEXT,
                    reviewer_name TEXT,
                    reviewer_location TEXT,
                    reviewer_country TEXT,
                    rating REAL,
                    title TEXT,
                    text TEXT,
                    travel_date TEXT,
                    review_date TEXT,
                    trip_type TEXT,
                    scraped_at TEXT
                )
            """)

            # Guide analysis table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS guide_analysis (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    review_id INTEGER NOT NULL,
                    mentions_guide BOOLEAN,
                    guide_names TEXT,
                    guide_keywords_found TEXT,
                    sentiment_score REAL,
                    sentiment_label TEXT,
                    guide_context TEXT,
                    FOREIGN KEY (review_id) REFERENCES reviews(id)
                )
            """)

            # Decision factors table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS decision_factors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    review_id INTEGER NOT NULL,
                    factor_type TEXT,
                    mentions TEXT,
                    sentiment_score REAL,
                    importance_score REAL,
                    FOREIGN KEY (review_id) REFERENCES reviews(id)
                )
            """)

            # Demographics table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS demographics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    review_id INTEGER NOT NULL,
                    country TEXT,
                    region TEXT,
                    travel_composition TEXT,
                    party_size INTEGER,
                    experience_level TEXT,
                    age_indicator TEXT,
                    FOREIGN KEY (review_id) REFERENCES reviews(id)
                )
            """)

            # Create indexes for common queries
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_reviews_source ON reviews(source)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_reviews_country ON reviews(reviewer_country)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_guide_mentions ON guide_analysis(mentions_guide)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_demographics_region ON demographics(region)")

            conn.commit()

    def insert_review(self, review: Review) -> int:
        """Insert a review, returns the ID. Skips if URL already exists."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    INSERT INTO reviews (
                        source, url, operator_name, reviewer_name, reviewer_location,
                        reviewer_country, rating, title, text, travel_date,
                        review_date, trip_type, scraped_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    review.source, review.url, review.operator_name, review.reviewer_name,
                    review.reviewer_location, review.reviewer_country, review.rating,
                    review.title, review.text, review.travel_date, review.review_date,
                    review.trip_type, review.scraped_at
                ))
                conn.commit()
                return cursor.lastrowid
            except sqlite3.IntegrityError:
                # URL already exists, get existing ID
                cursor.execute("SELECT id FROM reviews WHERE url = ?", (review.url,))
                row = cursor.fetchone()
                return row["id"] if row else -1

    def insert_guide_analysis(self, analysis: GuideAnalysis) -> int:
        """Insert guide analysis."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO guide_analysis (
                    review_id, mentions_guide, guide_names, guide_keywords_found,
                    sentiment_score, sentiment_label, guide_context
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                analysis.review_id, analysis.mentions_guide, analysis.guide_names,
                analysis.guide_keywords_found, analysis.sentiment_score,
                analysis.sentiment_label, analysis.guide_context
            ))
            conn.commit()
            return cursor.lastrowid

    def insert_decision_factor(self, factor: DecisionFactor) -> int:
        """Insert decision factor."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO decision_factors (
                    review_id, factor_type, mentions, sentiment_score, importance_score
                ) VALUES (?, ?, ?, ?, ?)
            """, (
                factor.review_id, factor.factor_type, factor.mentions,
                factor.sentiment_score, factor.importance_score
            ))
            conn.commit()
            return cursor.lastrowid

    def insert_demographic(self, demo: Demographic) -> int:
        """Insert demographic data."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO demographics (
                    review_id, country, region, travel_composition,
                    party_size, experience_level, age_indicator
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                demo.review_id, demo.country, demo.region, demo.travel_composition,
                demo.party_size, demo.experience_level, demo.age_indicator
            ))
            conn.commit()
            return cursor.lastrowid

    def get_reviews(self, source: Optional[str] = None, limit: int = 1000) -> list[Review]:
        """Get reviews, optionally filtered by source."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if source:
                cursor.execute(
                    "SELECT * FROM reviews WHERE source = ? LIMIT ?",
                    (source, limit)
                )
            else:
                cursor.execute("SELECT * FROM reviews LIMIT ?", (limit,))

            return [Review.from_dict(dict(row)) for row in cursor.fetchall()]

    def get_unanalyzed_reviews(self) -> list[Review]:
        """Get reviews that haven't been analyzed yet."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT r.* FROM reviews r
                LEFT JOIN guide_analysis ga ON r.id = ga.review_id
                WHERE ga.id IS NULL
            """)
            return [Review.from_dict(dict(row)) for row in cursor.fetchall()]

    def get_review_count(self, source: Optional[str] = None) -> int:
        """Get total review count."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if source:
                cursor.execute("SELECT COUNT(*) FROM reviews WHERE source = ?", (source,))
            else:
                cursor.execute("SELECT COUNT(*) FROM reviews")
            return cursor.fetchone()[0]

    def get_guide_mention_stats(self) -> dict:
        """Get statistics on guide mentions."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN mentions_guide = 1 THEN 1 ELSE 0 END) as with_guide,
                    AVG(CASE WHEN mentions_guide = 1 THEN sentiment_score END) as avg_sentiment
                FROM guide_analysis
            """)
            row = cursor.fetchone()
            return {
                "total_analyzed": row[0] or 0,
                "mentions_guide": row[1] or 0,
                "guide_mention_rate": (row[1] / row[0] * 100) if row[0] else 0,
                "avg_guide_sentiment": row[2] or 0,
            }

    def export_to_csv(self, output_path: str):
        """Export all data to CSV files."""
        import pandas as pd

        output_dir = Path(output_path)
        output_dir.mkdir(parents=True, exist_ok=True)

        with self._get_connection() as conn:
            # Export reviews
            df_reviews = pd.read_sql_query("SELECT * FROM reviews", conn)
            df_reviews.to_csv(output_dir / "reviews.csv", index=False)

            # Export guide analysis
            df_guides = pd.read_sql_query("SELECT * FROM guide_analysis", conn)
            df_guides.to_csv(output_dir / "guide_analysis.csv", index=False)

            # Export decision factors
            df_factors = pd.read_sql_query("SELECT * FROM decision_factors", conn)
            df_factors.to_csv(output_dir / "decision_factors.csv", index=False)

            # Export demographics
            df_demo = pd.read_sql_query("SELECT * FROM demographics", conn)
            df_demo.to_csv(output_dir / "demographics.csv", index=False)

        return output_dir

    def export_to_json(self, output_path: str):
        """Export all data to JSON files."""
        import json

        output_dir = Path(output_path)
        output_dir.mkdir(parents=True, exist_ok=True)

        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Export reviews
            cursor.execute("SELECT * FROM reviews")
            reviews = [dict(row) for row in cursor.fetchall()]
            with open(output_dir / "reviews.json", "w") as f:
                json.dump(reviews, f, indent=2)

            # Export guide analysis
            cursor.execute("SELECT * FROM guide_analysis")
            guides = [dict(row) for row in cursor.fetchall()]
            with open(output_dir / "guide_analysis.json", "w") as f:
                json.dump(guides, f, indent=2)

            # Export decision factors
            cursor.execute("SELECT * FROM decision_factors")
            factors = [dict(row) for row in cursor.fetchall()]
            with open(output_dir / "decision_factors.json", "w") as f:
                json.dump(factors, f, indent=2)

            # Export demographics
            cursor.execute("SELECT * FROM demographics")
            demo = [dict(row) for row in cursor.fetchall()]
            with open(output_dir / "demographics.json", "w") as f:
                json.dump(demo, f, indent=2)

        return output_dir
