# Safari Review Fetcher

A web scraper and analysis tool for safari reviews from TripAdvisor and Safaribookings. Analyzes reviews to understand:

- **Safari guide impact**: How often guides are mentioned and sentiment around them
- **Purchasing decision factors**: Price, wildlife, vehicles, accommodation, safety, etc.
- **Demographics**: Reviewer location (NA/UK/EU focus), travel composition, experience level

## Features

- Scrapes reviews from Safaribookings.com and TripAdvisor
- Anti-bot detection with stealth browsing and CAPTCHA pause-and-notify
- SQLite database for persistent storage
- NLP-based analysis for guide mentions, sentiment, and decision factors
- CSV and JSON export for further analysis
- Resume capability for interrupted scraping sessions

## Installation

### Prerequisites

- Python 3.9+
- Node.js (for Playwright browsers)

### Setup

```bash
# Clone/navigate to the project
cd join-review-fetcher

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install --upgrade pip
pip install playwright pandas spacy textblob rich click fastapi uvicorn websockets

# Install Playwright browsers
playwright install chromium
```

## Project Structure

```
join-review-fetcher/
├── pyproject.toml          # Project configuration
├── README.md               # This file
├── src/
│   ├── __init__.py
│   ├── cli.py              # Command-line interface
│   ├── scrapers/
│   │   ├── base.py         # Base scraper with anti-bot features
│   │   ├── safaribookings.py
│   │   ├── tripadvisor.py
│   │   ├── country_codes.py    # ISO country code mapping
│   │   └── validation.py       # Review validation and error tracking
│   ├── database/
│   │   ├── models.py       # Data models (Review, GuideAnalysis, etc.)
│   │   └── connection.py   # SQLite database operations
│   ├── analysis/
│   │   ├── guide_analyzer.py      # Guide mention detection
│   │   ├── decision_factors.py    # Purchasing factor extraction
│   │   └── demographics.py        # Demographic classification
│   └── web/                # Web UI module
│       ├── app.py              # FastAPI application
│       ├── routes.py           # REST API endpoints
│       ├── websocket.py        # WebSocket connection manager
│       ├── scraper_runner.py   # Background scraper execution
│       ├── sleep_manager.py    # macOS sleep prevention
│       └── static/             # Frontend assets
│           ├── index.html      # Tailwind CSS UI
│           └── js/app.js       # Alpine.js application
├── data/
│   └── reviews.db          # SQLite database (created on first run)
└── output/
    └── reports/            # Exported CSV/JSON files
```

## Usage

### Quick Start

```bash
source venv/bin/activate

# Scrape reviews (runs with visible browser for CAPTCHA solving)
python -m src.cli scrape --source safaribookings --max-operators 10 --no-headless

# Run analysis on scraped reviews
python -m src.cli analyze

# View statistics
python -m src.cli stats

# Export data
python -m src.cli export --format both
```

### CLI Commands

#### `scrape` - Scrape reviews from websites

```bash
python -m src.cli scrape [OPTIONS]

Options:
  --source [safaribookings|tripadvisor|all]  Source to scrape (default: all)
  --max-operators INTEGER                     Max operators to scrape (default: 50)
  --max-reviews INTEGER                       Max reviews per operator (default: 50)
  --headless / --no-headless                 Run browser headless (default: headless)
  --resume / --no-resume                     Resume from last position (default: resume)
```

**Important**: Use `--no-headless` to see the browser and solve CAPTCHAs when they appear.

#### `analyze` - Run analysis on scraped reviews

```bash
python -m src.cli analyze
```

Analyzes all reviews for:
- Guide mentions and sentiment
- Decision factors (price, wildlife, safety, etc.)
- Demographics (region, travel type, experience)

#### `stats` - View statistics

```bash
python -m src.cli stats
```

Shows:
- Total review counts by source
- Guide mention rate and sentiment
- Key insights

#### `export` - Export data to CSV/JSON

```bash
python -m src.cli export [OPTIONS]

Options:
  --format [csv|json|both]  Output format (default: both)
  --output TEXT             Output directory (default: output/reports)
```

#### `report` - Generate analysis report

```bash
python -m src.cli report
```

#### `clear-progress` - Reset scraper progress

```bash
python -m src.cli clear-progress --source safaribookings
```

### Web UI

The scraper includes a web-based UI for real-time monitoring and control.

#### Starting the Web Server

```bash
# Using the CLI command
python -m src.cli web

# Or using uvicorn directly
python3 -m uvicorn src.web.app:app --host 127.0.0.1 --port 8000

# With auto-reload for development
python3 -m uvicorn src.web.app:app --host 127.0.0.1 --port 8000 --reload
```

Then open http://127.0.0.1:8000 in your browser.

#### Web UI Features

- **Real-time Progress Monitoring**: WebSocket-based live updates during scraping
- **Control Panel**: Start/stop scraping, configure max operators and reviews
- **Sleep Prevention**: Automatically prevents macOS from sleeping during long scrapes
- **Database Statistics**: View review counts, countries, operators in real-time
- **Activity Log**: Scrolling log of all scraper events
- **Quality Metrics**: Parse success rate, failed parses, low confidence counts

#### CLI Options

```bash
python -m src.cli web [OPTIONS]

Options:
  --host TEXT      Host to bind to (default: 127.0.0.1)
  --port INTEGER   Port to run server on (default: 8000)
  --reload         Enable auto-reload for development
```

### Programmatic Usage

```python
import asyncio
from src.scrapers import SafaribookingsScraper, TripAdvisorScraper
from src.database import Database
from src.analysis import GuideAnalyzer, DecisionFactorAnalyzer, DemographicsAnalyzer

async def main():
    # Initialize database
    db = Database()

    # Scrape Safaribookings
    scraper = SafaribookingsScraper(
        headless=False,  # Show browser for CAPTCHA solving
        min_delay=3,     # Minimum delay between requests (seconds)
        max_delay=6      # Maximum delay between requests (seconds)
    )

    reviews = await scraper.scrape_all(
        max_operators=20,
        max_reviews_per_operator=30,
        resume=True  # Resume from last position if interrupted
    )

    # Save to database
    for review in reviews:
        db.insert_review(review)

    # Run analysis
    guide_analyzer = GuideAnalyzer()
    factor_analyzer = DecisionFactorAnalyzer()
    demo_analyzer = DemographicsAnalyzer()

    for review in db.get_reviews():
        # Guide analysis
        guide_result = guide_analyzer.analyze(review)
        db.insert_guide_analysis(guide_result)

        # Decision factors
        factors = factor_analyzer.analyze(review)
        for factor in factors:
            db.insert_decision_factor(factor)

        # Demographics
        demo = demo_analyzer.analyze(review)
        db.insert_demographic(demo)

    # Export results
    db.export_to_csv("output/reports/csv")
    db.export_to_json("output/reports/json")

    # Get statistics
    stats = db.get_guide_mention_stats()
    print(f"Guide mention rate: {stats['guide_mention_rate']:.1f}%")
    print(f"Average guide sentiment: {stats['avg_guide_sentiment']:.2f}")

asyncio.run(main())
```

## CAPTCHA Handling

Both Safaribookings and TripAdvisor have anti-bot measures. When a CAPTCHA is detected:

1. The scraper pauses and displays a notification
2. Solve the CAPTCHA manually in the browser window
3. The scraper automatically resumes when the CAPTCHA is solved

**Recommendation**: Always run with `--no-headless` flag to handle CAPTCHAs.

## Analysis Details

### Guide Detection

The analyzer looks for:
- Keywords: guide, driver, ranger, tracker, spotter, etc.
- Guide names extraction using pattern matching
- Sentiment analysis around guide mentions

### Decision Factors

Extracts mentions of:
- **Price/Value**: cost, expensive, affordable, worth, budget
- **Wildlife**: Big Five, specific animals, sightings
- **Vehicle/Equipment**: 4x4, pop-top roof, comfortable
- **Accommodation**: lodge, camp, tent, food, pool
- **Safety**: safe, secure, reliable, trust
- **Communication**: booking, response, organized
- **Group Size**: private, small group, exclusive

### Demographics

Classifies reviewers by:
- **Region**: North America, UK, Europe, Other
- **Travel Composition**: solo, couple, family, friends, group
- **Experience Level**: first safari, repeat visitor
- **Age Indicators**: retired, honeymoon, family with kids

## Output Files

After running `export`, you'll find:

```
output/reports/
├── csv/
│   ├── reviews.csv           # All scraped reviews
│   ├── guide_analysis.csv    # Guide mention analysis
│   ├── decision_factors.csv  # Extracted decision factors
│   └── demographics.csv      # Demographic classifications
└── json/
    ├── reviews.json
    ├── guide_analysis.json
    ├── decision_factors.json
    └── demographics.json
```

## Tips for Large-Scale Scraping

1. **Use longer delays**: Increase `min_delay` and `max_delay` to avoid detection
2. **Run in batches**: Scrape a few operators at a time, then pause
3. **Resume capability**: The scraper saves progress automatically
4. **Rotate sessions**: Close and restart the browser periodically
5. **Monitor for blocks**: Watch for increased CAPTCHA frequency

## Troubleshooting

### "CAPTCHA DETECTED" message
- Run with `--no-headless` and solve the CAPTCHA manually
- Increase delays between requests
- Try again later if repeatedly blocked

### No reviews found
- The website structure may have changed
- Check if the page loads correctly in a regular browser
- Review the selectors in `src/scrapers/safaribookings.py` or `tripadvisor.py`

### Database locked error
- Close any other processes using the database
- Delete `data/reviews.db` to start fresh (loses all data)

## Legal Disclaimer

This tool is for personal/educational use only. Web scraping may violate the Terms of Service of the target websites. Use responsibly and respect rate limits. The authors are not responsible for any misuse of this tool.

## License

MIT License
