"""Command-line interface for safari review scraper and analyzer."""
import asyncio
import json
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.panel import Panel

from .database import Database
from .scrapers import SafaribookingsScraper, TripAdvisorScraper
from .analysis import GuideAnalyzer, DecisionFactorAnalyzer, DemographicsAnalyzer

console = Console()


@click.group()
@click.option("--db", default="data/reviews.db", help="Database path")
@click.pass_context
def main(ctx, db):
    """Safari Review Fetcher - Scrape and analyze safari reviews."""
    ctx.ensure_object(dict)
    ctx.obj["db"] = Database(db)


@main.command()
@click.option("--source", type=click.Choice(["safaribookings", "tripadvisor", "all"]), default="all")
@click.option("--max-operators", default=50, help="Maximum operators to scrape")
@click.option("--max-reviews", default=50, help="Maximum reviews per operator")
@click.option("--headless/--no-headless", default=True, help="Run browser headless")
@click.option("--resume/--no-resume", default=True, help="Resume from last position")
@click.pass_context
def scrape(ctx, source, max_operators, max_reviews, headless, resume):
    """Scrape reviews from safari booking sites."""
    db = ctx.obj["db"]

    async def run_scrapers():
        total_reviews = 0

        if source in ["safaribookings", "all"]:
            console.print("\n[bold blue]Scraping Safaribookings.com...[/]")
            scraper = SafaribookingsScraper(headless=headless)

            try:
                reviews = await scraper.scrape_all(
                    max_operators=max_operators,
                    max_reviews_per_operator=max_reviews,
                    resume=resume,
                )

                for review in reviews:
                    db.insert_review(review)

                total_reviews += len(reviews)
                console.print(f"[green]Saved {len(reviews)} reviews from Safaribookings[/]")

            except Exception as e:
                console.print(f"[red]Error scraping Safaribookings: {e}[/]")

        if source in ["tripadvisor", "all"]:
            console.print("\n[bold blue]Scraping TripAdvisor...[/]")
            scraper = TripAdvisorScraper(headless=headless)

            try:
                reviews = await scraper.scrape_all(
                    regions=["kenya", "tanzania"],
                    max_operators=max_operators,
                    max_reviews_per_operator=max_reviews,
                    resume=resume,
                )

                for review in reviews:
                    db.insert_review(review)

                total_reviews += len(reviews)
                console.print(f"[green]Saved {len(reviews)} reviews from TripAdvisor[/]")

            except Exception as e:
                console.print(f"[red]Error scraping TripAdvisor: {e}[/]")

        return total_reviews

    total = asyncio.run(run_scrapers())
    console.print(f"\n[bold green]Total reviews scraped: {total}[/]")
    console.print(f"[dim]Database: {db.db_path}[/]")


@main.command()
@click.pass_context
def analyze(ctx):
    """Run analysis on all scraped reviews."""
    db = ctx.obj["db"]

    console.print("\n[bold blue]Running Analysis...[/]")

    # Get unanalyzed reviews
    reviews = db.get_unanalyzed_reviews()

    if not reviews:
        reviews = db.get_reviews()

    if not reviews:
        console.print("[yellow]No reviews found. Run 'scrape' first.[/]")
        return

    console.print(f"Analyzing {len(reviews)} reviews...")

    guide_analyzer = GuideAnalyzer()
    factor_analyzer = DecisionFactorAnalyzer()
    demo_analyzer = DemographicsAnalyzer()

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Analyzing reviews...", total=len(reviews))

        for review in reviews:
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

            progress.advance(task)

    console.print("[green]Analysis complete![/]")


@main.command()
@click.pass_context
def stats(ctx):
    """Show statistics and insights."""
    db = ctx.obj["db"]

    # Overview stats
    console.print(Panel.fit("[bold]Safari Review Analysis[/]", border_style="blue"))

    # Review counts
    total = db.get_review_count()
    safaribookings = db.get_review_count("safaribookings")
    tripadvisor = db.get_review_count("tripadvisor")

    table = Table(title="Review Counts")
    table.add_column("Source", style="cyan")
    table.add_column("Count", style="green")
    table.add_row("Safaribookings", str(safaribookings))
    table.add_row("TripAdvisor", str(tripadvisor))
    table.add_row("[bold]Total[/]", f"[bold]{total}[/]")
    console.print(table)

    # Guide mention stats
    guide_stats = db.get_guide_mention_stats()

    if guide_stats["total_analyzed"] > 0:
        console.print("\n")
        table = Table(title="Safari Guide Analysis")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")
        table.add_row("Reviews Analyzed", str(guide_stats["total_analyzed"]))
        table.add_row("Mention Guide", str(guide_stats["mentions_guide"]))
        table.add_row(
            "Guide Mention Rate",
            f"{guide_stats['guide_mention_rate']:.1f}%"
        )
        table.add_row(
            "Avg Guide Sentiment",
            f"{guide_stats['avg_guide_sentiment']:.2f}"
        )
        console.print(table)


@main.command()
@click.option("--format", "fmt", type=click.Choice(["csv", "json", "both"]), default="both")
@click.option("--output", default="output/reports", help="Output directory")
@click.pass_context
def export(ctx, fmt, output):
    """Export data to CSV and/or JSON."""
    db = ctx.obj["db"]
    output_dir = Path(output)

    if fmt in ["csv", "both"]:
        db.export_to_csv(output_dir / "csv")
        console.print(f"[green]CSV files exported to {output_dir / 'csv'}[/]")

    if fmt in ["json", "both"]:
        db.export_to_json(output_dir / "json")
        console.print(f"[green]JSON files exported to {output_dir / 'json'}[/]")


@main.command()
@click.pass_context
def report(ctx):
    """Generate comprehensive analysis report."""
    db = ctx.obj["db"]

    console.print(Panel.fit(
        "[bold]Safari Review Analysis Report[/]",
        border_style="green"
    ))

    total = db.get_review_count()
    if total == 0:
        console.print("[yellow]No reviews in database. Run 'scrape' first.[/]")
        return

    guide_stats = db.get_guide_mention_stats()

    # Key Findings
    console.print("\n[bold cyan]Key Findings:[/]")
    console.print(f"  Total reviews analyzed: {total}")

    if guide_stats["total_analyzed"] > 0:
        mention_rate = guide_stats["guide_mention_rate"]
        sentiment = guide_stats["avg_guide_sentiment"]

        console.print(f"\n  [bold]Safari Guide Impact:[/]")
        console.print(f"    - {mention_rate:.1f}% of reviews mention their guide")

        if sentiment > 0.3:
            console.print(f"    - Guide mentions are [green]highly positive[/] (sentiment: {sentiment:.2f})")
        elif sentiment > 0:
            console.print(f"    - Guide mentions are [green]positive[/] (sentiment: {sentiment:.2f})")
        else:
            console.print(f"    - Guide mentions are [yellow]neutral/mixed[/] (sentiment: {sentiment:.2f})")

    # Decision factors summary from database
    console.print("\n  [bold]Top Decision Factors:[/]")
    console.print("    (Run 'export' to see detailed factor analysis)")

    # Demographics summary
    console.print("\n  [bold]Reviewer Demographics:[/]")
    console.print("    (Run 'export' to see detailed demographic breakdown)")

    console.print("\n[dim]Use 'safari-reviews export' to get full data exports[/]")


@main.command()
@click.option("--source", type=click.Choice(["safaribookings", "tripadvisor"]), required=True)
@click.pass_context
def clear_progress(ctx, source):
    """Clear scraper progress for a source."""
    from .scrapers.base import ScraperState

    state = ScraperState()
    state.clear(source)
    console.print(f"[green]Cleared progress for {source}[/]")


@main.command()
@click.option("--host", default="127.0.0.1", help="Host to bind to")
@click.option("--port", default=8000, help="Port to run server on")
@click.option("--reload", is_flag=True, help="Enable auto-reload for development")
def web(host, port, reload):
    """Start the web UI server."""
    import uvicorn

    console.print(Panel.fit(
        f"[bold]Safari Review Scraper Web UI[/]\n"
        f"[dim]Starting server at http://{host}:{port}[/]",
        border_style="green"
    ))

    uvicorn.run(
        "src.web.app:app",
        host=host,
        port=port,
        reload=reload,
    )


if __name__ == "__main__":
    main()
