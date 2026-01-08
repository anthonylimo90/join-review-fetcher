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
                    scraped_at TEXT,
                    reviewer_contributions INTEGER DEFAULT 0,
                    reviewer_helpful_votes INTEGER DEFAULT 0,
                    helpful_votes INTEGER DEFAULT 0,
                    review_id_source TEXT,
                    age_range TEXT,
                    parks_visited TEXT DEFAULT '[]',
                    wildlife_sightings TEXT DEFAULT '[]',
                    guide_names_mentioned TEXT DEFAULT '[]',
                    safari_duration_days INTEGER,
                    parsing_confidence REAL DEFAULT 1.0,
                    raw_text_block TEXT,
                    parse_warnings TEXT DEFAULT '[]'
                )
            """)

            # Run migration for existing databases
            self._migrate_db(cursor)

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

            # Scrape runs table for run history
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS scrape_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TEXT NOT NULL,
                    ended_at TEXT,
                    status TEXT NOT NULL DEFAULT 'running',
                    source TEXT,
                    config_json TEXT,
                    operators_total INTEGER DEFAULT 0,
                    operators_completed INTEGER DEFAULT 0,
                    reviews_collected INTEGER DEFAULT 0,
                    errors_json TEXT DEFAULT '[]'
                )
            """)

            # Create indexes for common queries
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_reviews_source ON reviews(source)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_reviews_country ON reviews(reviewer_country)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_reviews_operator ON reviews(operator_name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_reviews_scraped_at ON reviews(scraped_at)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_reviews_rating ON reviews(rating)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_reviews_id_desc ON reviews(id DESC)")

            # Foreign key indexes for JOINs
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_guide_analysis_review_id ON guide_analysis(review_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_decision_factors_review_id ON decision_factors(review_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_demographics_review_id ON demographics(review_id)")

            # Other indexes
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_guide_mentions ON guide_analysis(mentions_guide)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_demographics_region ON demographics(region)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_scrape_runs_status ON scrape_runs(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_scrape_runs_started_at ON scrape_runs(started_at DESC)")

            conn.commit()

    def _migrate_db(self, cursor):
        """Apply database migrations for new columns on existing databases."""
        new_columns = [
            ("reviewer_contributions", "INTEGER DEFAULT 0"),
            ("reviewer_helpful_votes", "INTEGER DEFAULT 0"),
            ("helpful_votes", "INTEGER DEFAULT 0"),
            ("review_id_source", "TEXT"),
            ("age_range", "TEXT"),
            ("parks_visited", "TEXT DEFAULT '[]'"),
            ("wildlife_sightings", "TEXT DEFAULT '[]'"),
            ("guide_names_mentioned", "TEXT DEFAULT '[]'"),
            ("safari_duration_days", "INTEGER"),
            ("parsing_confidence", "REAL DEFAULT 1.0"),
            ("raw_text_block", "TEXT"),
            ("parse_warnings", "TEXT DEFAULT '[]'"),
        ]

        for col_name, col_def in new_columns:
            try:
                cursor.execute(f"ALTER TABLE reviews ADD COLUMN {col_name} {col_def}")
            except sqlite3.OperationalError:
                pass  # Column already exists

    def insert_review(self, review: Review) -> int:
        """Insert a review, returns the ID. Skips if URL already exists."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    INSERT INTO reviews (
                        source, url, operator_name, reviewer_name, reviewer_location,
                        reviewer_country, rating, title, text, travel_date,
                        review_date, trip_type, scraped_at,
                        reviewer_contributions, reviewer_helpful_votes, helpful_votes,
                        review_id_source, age_range, parks_visited, wildlife_sightings,
                        guide_names_mentioned, safari_duration_days, parsing_confidence,
                        raw_text_block, parse_warnings
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    review.source, review.url, review.operator_name, review.reviewer_name,
                    review.reviewer_location, review.reviewer_country, review.rating,
                    review.title, review.text, review.travel_date, review.review_date,
                    review.trip_type, review.scraped_at,
                    review.reviewer_contributions, review.reviewer_helpful_votes,
                    review.helpful_votes, review.review_id_source, review.age_range,
                    review.parks_visited, review.wildlife_sightings,
                    review.guide_names_mentioned, review.safari_duration_days,
                    review.parsing_confidence, review.raw_text_block, review.parse_warnings
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

    def get_guide_intelligence(self) -> dict:
        """Get comprehensive guide intelligence analysis."""
        import re
        import json
        from collections import Counter

        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Basic stats
            cursor.execute("SELECT COUNT(*) FROM reviews")
            total_reviews = cursor.fetchone()[0] or 0

            cursor.execute("""
                SELECT COUNT(*) FROM reviews
                WHERE LOWER(text) LIKE '%guide%' OR LOWER(text) LIKE '%driver%'
            """)
            guide_mentions = cursor.fetchone()[0] or 0

            # Rating comparison
            cursor.execute("""
                SELECT AVG(rating) FROM reviews
                WHERE LOWER(text) LIKE '%guide%' OR LOWER(text) LIKE '%driver%'
            """)
            avg_with_guide = cursor.fetchone()[0] or 0

            cursor.execute("""
                SELECT AVG(rating) FROM reviews
                WHERE LOWER(text) NOT LIKE '%guide%' AND LOWER(text) NOT LIKE '%driver%'
            """)
            avg_without_guide = cursor.fetchone()[0] or 0

            # Rating distribution for guide-mentioning reviews
            cursor.execute("""
                SELECT
                    CASE
                        WHEN rating >= 4.5 THEN '5_stars'
                        WHEN rating >= 3.5 THEN '4_stars'
                        WHEN rating >= 2.5 THEN '3_stars'
                        WHEN rating >= 1.5 THEN '2_stars'
                        ELSE '1_star'
                    END as rating_group,
                    COUNT(*)
                FROM reviews
                WHERE LOWER(text) LIKE '%guide%' OR LOWER(text) LIKE '%driver%'
                GROUP BY rating_group
                ORDER BY rating_group DESC
            """)
            rating_distribution = {row[0]: row[1] for row in cursor.fetchall()}

            # Get all review texts for quality analysis
            cursor.execute("""
                SELECT text, rating, operator_name, guide_names_mentioned
                FROM reviews
                WHERE text IS NOT NULL
                AND (LOWER(text) LIKE '%guide%' OR LOWER(text) LIKE '%driver%')
            """)
            reviews_with_guides = cursor.fetchall()

            # Quality patterns to extract
            quality_patterns = [
                (r'\b(knowledgeable|knowledge)\b', 'Knowledgeable'),
                (r'\b(experienced|experience)\b', 'Experienced'),
                (r'\b(friendly|warm|welcoming)\b', 'Friendly'),
                (r'\b(professional|professionalism)\b', 'Professional'),
                (r'\b(helpful|accommodating)\b', 'Helpful'),
                (r'\b(patient|patience)\b', 'Patient'),
                (r'\b(informative|explained|explaining)\b', 'Informative'),
                (r'\b(punctual|on time|timely)\b', 'Punctual'),
                (r'\b(safe|safety|careful)\b', 'Safety-conscious'),
                (r'\b(spot|spotting|spotted)\b', 'Wildlife Spotting'),
                (r'\b(passionate|enthusiasm|enthusiastic)\b', 'Passionate'),
                (r'\b(recommend|recommended)\b', 'Highly Recommended'),
            ]

            # Sentiment phrases
            positive_phrases = [
                'excellent guide', 'amazing guide', 'best guide', 'fantastic guide',
                'wonderful guide', 'great guide', 'incredible guide', 'outstanding guide',
                'our guide was amazing', 'our guide was excellent', 'our guide was fantastic',
                'highly recommend', 'special thanks', 'shout out to', 'hats off to',
                'couldn\'t have asked for', 'above and beyond', 'made our trip',
                'highlight of', 'best part of'
            ]

            negative_phrases = [
                'poor guide', 'bad guide', 'disappointing guide', 'guide was rude',
                'unprofessional', 'inexperienced', 'guide didn\'t', 'guide was late',
                'wouldn\'t recommend', 'not happy with'
            ]

            # Analyze reviews
            quality_counts = Counter()
            positive_count = 0
            negative_count = 0
            neutral_count = 0
            operator_guide_scores = {}
            named_guides = Counter()

            for text, rating, operator, guides_json in reviews_with_guides:
                if not text:
                    continue
                text_lower = text.lower()

                # Count qualities
                for pattern, quality in quality_patterns:
                    if re.search(pattern, text_lower):
                        quality_counts[quality] += 1

                # Sentiment analysis
                has_positive = any(phrase in text_lower for phrase in positive_phrases)
                has_negative = any(phrase in text_lower for phrase in negative_phrases)

                if has_positive and not has_negative:
                    positive_count += 1
                elif has_negative and not has_positive:
                    negative_count += 1
                else:
                    neutral_count += 1

                # Operator scores
                if operator:
                    if operator not in operator_guide_scores:
                        operator_guide_scores[operator] = {'total': 0, 'sum': 0, 'positive': 0}
                    operator_guide_scores[operator]['total'] += 1
                    operator_guide_scores[operator]['sum'] += rating or 0
                    if has_positive:
                        operator_guide_scores[operator]['positive'] += 1

                # Named guides (filter out common false positives)
                false_positives = {'who', 'you', 'and', 'the', 'our', 'we', 'they', 'he', 'she',
                                   'it', 'was', 'were', 'been', 'being', 'have', 'has', 'had',
                                   'success', 'gave', 'made', 'took', 'got', 'went', 'came'}
                if guides_json:
                    try:
                        guides = json.loads(guides_json) if isinstance(guides_json, str) else guides_json
                        for guide in guides:
                            if guide and len(guide) > 2 and guide.lower() not in false_positives:
                                named_guides[guide] += 1
                    except:
                        pass

            # Calculate operator rankings
            operator_rankings = []
            for op, scores in operator_guide_scores.items():
                if scores['total'] >= 5:  # Minimum 5 reviews
                    avg_rating = scores['sum'] / scores['total']
                    positive_rate = (scores['positive'] / scores['total']) * 100
                    operator_rankings.append({
                        'operator': op,
                        'reviews_with_guides': scores['total'],
                        'avg_rating': round(avg_rating, 2),
                        'positive_rate': round(positive_rate, 1)
                    })

            operator_rankings.sort(key=lambda x: (x['positive_rate'], x['avg_rating']), reverse=True)

            # Top guides
            top_guides = [{'name': name, 'mentions': count}
                          for name, count in named_guides.most_common(20)]

            # Quality breakdown for chart
            qualities = [{'quality': q, 'count': c}
                         for q, c in quality_counts.most_common(12)]

            return {
                'overview': {
                    'total_reviews': total_reviews,
                    'reviews_mentioning_guides': guide_mentions,
                    'guide_mention_rate': round((guide_mentions / total_reviews * 100), 1) if total_reviews else 0,
                    'avg_rating_with_guide': round(avg_with_guide, 2) if avg_with_guide else 0,
                    'avg_rating_without_guide': round(avg_without_guide, 2) if avg_without_guide else 0,
                    'rating_impact': round((avg_with_guide - avg_without_guide), 2) if avg_with_guide and avg_without_guide else 0,
                },
                'sentiment': {
                    'positive': positive_count,
                    'negative': negative_count,
                    'neutral': neutral_count,
                    'positive_rate': round((positive_count / guide_mentions * 100), 1) if guide_mentions else 0,
                },
                'rating_distribution': rating_distribution,
                'qualities': qualities,
                'top_guides': top_guides,
                'top_operators': operator_rankings[:10],
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

    # Scrape runs methods
    def create_scrape_run(self, source: str, config: dict) -> int:
        """Create a new scrape run record."""
        import json
        from datetime import datetime

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO scrape_runs (started_at, status, source, config_json)
                VALUES (?, 'running', ?, ?)
            """, (datetime.now().isoformat(), source, json.dumps(config)))
            conn.commit()
            return cursor.lastrowid

    def update_scrape_run(self, run_id: int, **kwargs):
        """Update a scrape run record."""
        import json
        from datetime import datetime

        with self._get_connection() as conn:
            cursor = conn.cursor()

            updates = []
            values = []

            if 'status' in kwargs:
                updates.append("status = ?")
                values.append(kwargs['status'])
                if kwargs['status'] in ('completed', 'stopped', 'failed'):
                    updates.append("ended_at = ?")
                    values.append(datetime.now().isoformat())

            if 'operators_total' in kwargs:
                updates.append("operators_total = ?")
                values.append(kwargs['operators_total'])

            if 'operators_completed' in kwargs:
                updates.append("operators_completed = ?")
                values.append(kwargs['operators_completed'])

            if 'reviews_collected' in kwargs:
                updates.append("reviews_collected = ?")
                values.append(kwargs['reviews_collected'])

            if 'errors' in kwargs:
                updates.append("errors_json = ?")
                values.append(json.dumps(kwargs['errors']))

            if updates:
                values.append(run_id)
                cursor.execute(f"""
                    UPDATE scrape_runs SET {', '.join(updates)} WHERE id = ?
                """, values)
                conn.commit()

    def get_scrape_runs(self, limit: int = 20) -> list:
        """Get recent scrape runs."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM scrape_runs ORDER BY started_at DESC LIMIT ?
            """, (limit,))
            return [dict(row) for row in cursor.fetchall()]

    def get_scrape_run(self, run_id: int) -> Optional[dict]:
        """Get a specific scrape run."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM scrape_runs WHERE id = ?", (run_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
