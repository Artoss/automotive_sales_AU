"""
Motor Vehicles Scraper -- CLI entry point and orchestrator.

Usage:
    uv run motor-vehicles marklines run       # Full Marklines pipeline
    uv run motor-vehicles marklines download  # Fetch Marklines HTML pages
    uv run motor-vehicles marklines parse     # Parse saved HTML
    uv run motor-vehicles fcai run            # Full FCAI pipeline
    uv run motor-vehicles fcai download       # Download FCAI PDFs
    uv run motor-vehicles fcai parse          # Extract data from PDFs
    uv run motor-vehicles run                 # Full pipeline (both sources)
    uv run motor-vehicles migrate             # Run SQL migrations
    uv run motor-vehicles status              # Show scrape run history
    uv run motor-vehicles export              # Export to CSV
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import click

from motor_vehicles.config import AppConfig, load_config
from motor_vehicles.utils.logging import setup_logging

logger = logging.getLogger("motor_vehicles.main")


# ---------------------------------------------------------------------------
# CLI group
# ---------------------------------------------------------------------------

@click.group(invoke_without_command=True)
@click.option("--config", "config_path", default="config.yaml", help="Path to config.yaml")
@click.option("--mode", type=click.Choice(["full", "incremental"]), help="Override run mode")
@click.pass_context
def cli(ctx, config_path, mode):
    """Motor Vehicles Scraper - Australian automotive sales data from Marklines and FCAI."""
    ctx.ensure_object(dict)
    config = load_config(config_path)
    setup_logging(config.logging)

    if mode:
        config.run_mode = mode

    ctx.obj["config"] = config

    if ctx.invoked_subcommand is None:
        _run_full_pipeline(config)


# ---------------------------------------------------------------------------
# Marklines subgroup
# ---------------------------------------------------------------------------

@cli.group()
@click.pass_context
def marklines(ctx):
    """Marklines scraper commands."""
    pass


@marklines.command(name="download")
@click.pass_context
def marklines_download(ctx):
    """Fetch Marklines HTML pages and save locally."""
    config: AppConfig = ctx.obj["config"]
    _marklines_download(config)


@marklines.command(name="parse")
@click.pass_context
def marklines_parse(ctx):
    """Parse saved Marklines HTML files."""
    config: AppConfig = ctx.obj["config"]
    _marklines_parse(config)


@marklines.command(name="run")
@click.pass_context
def marklines_run(ctx):
    """Full Marklines pipeline: fetch, parse, load."""
    config: AppConfig = ctx.obj["config"]
    _marklines_full(config)


# ---------------------------------------------------------------------------
# FCAI subgroup
# ---------------------------------------------------------------------------

@cli.group()
@click.pass_context
def fcai(ctx):
    """FCAI scraper commands."""
    pass


@fcai.command(name="download")
@click.option("--year", type=int, help="Download only this year")
@click.option("--month", help="Download only this month (e.g. january)")
@click.pass_context
def fcai_download(ctx, year, month):
    """Download FCAI PDF publications."""
    config: AppConfig = ctx.obj["config"]
    _fcai_download(config, year=year, month=month)


@fcai.command(name="parse")
@click.pass_context
def fcai_parse(ctx):
    """Extract data from downloaded FCAI PDFs."""
    config: AppConfig = ctx.obj["config"]
    _fcai_parse(config)


@fcai.command(name="run")
@click.option("--year", type=int, help="Process only this year")
@click.option("--month", help="Process only this month (e.g. january)")
@click.pass_context
def fcai_run(ctx, year, month):
    """Full FCAI pipeline: download, extract, load."""
    config: AppConfig = ctx.obj["config"]
    _fcai_full(config, year=year, month=month)


@fcai.command(name="articles")
@click.option("--url", help="Process a single article URL")
@click.option("--list-only", is_flag=True, help="List article URLs without processing")
@click.option("--process-all", is_flag=True, help="Process all monthly sales articles")
@click.pass_context
def fcai_articles(ctx, url, list_only, process_all):
    """Scrape FCAI media release articles for table images."""
    config: AppConfig = ctx.obj["config"]
    _fcai_articles(config, url=url, list_only=list_only, process_all=process_all)


@fcai.command(name="build-state-sales")
@click.pass_context
def fcai_build_state_sales(ctx):
    """Extract State/Territory time-series from article tables."""
    config: AppConfig = ctx.obj["config"]
    _fcai_build_state_sales(config)


# ---------------------------------------------------------------------------
# Top-level commands
# ---------------------------------------------------------------------------

@cli.command()
@click.pass_context
def run(ctx):
    """Full pipeline: both Marklines and FCAI."""
    config: AppConfig = ctx.obj["config"]
    _run_full_pipeline(config)


@cli.command()
@click.pass_context
def migrate(ctx):
    """Run database migrations."""
    config: AppConfig = ctx.obj["config"]
    _run_migrate(config)


@cli.command()
@click.pass_context
def status(ctx):
    """Show recent scrape runs and stats."""
    config: AppConfig = ctx.obj["config"]
    _run_status(config)


@cli.command()
@click.pass_context
def update(ctx):
    """Monthly update: fetch new data, process, report coverage."""
    config: AppConfig = ctx.obj["config"]
    _run_monthly_update(config)


@cli.command(name="export")
@click.option("--source", type=click.Choice(["marklines", "fcai", "all"]), default="all")
@click.option("--format", "fmt", type=click.Choice(["csv"]), default="csv")
@click.pass_context
def export_cmd(ctx, source, fmt):
    """Export data to CSV."""
    config: AppConfig = ctx.obj["config"]
    _run_export(config, source=source, fmt=fmt)


# ---------------------------------------------------------------------------
# Implementation: Marklines
# ---------------------------------------------------------------------------

def _marklines_download(config: AppConfig) -> None:
    """Fetch and save Marklines HTML pages."""
    from motor_vehicles.scraping.marklines_client import MarklinesClient

    data_dir = Path("data/marklines")
    data_dir.mkdir(parents=True, exist_ok=True)

    client = MarklinesClient(config.http, config.marklines)
    try:
        pages = client.fetch_all_pages()
        for url, html in pages.items():
            # Save as local HTML file
            safe_name = url.split("/")[-1] or "current"
            safe_name = safe_name.replace("?", "_").replace("&", "_")
            filepath = data_dir / f"{safe_name}.html"
            filepath.write_text(html, encoding="utf-8")
            click.echo(f"  Saved {filepath} ({len(html):,} bytes)")
        click.echo(f"Downloaded {len(pages)} pages")
    finally:
        client.close()


def _marklines_parse(config: AppConfig) -> None:
    """Parse saved Marklines HTML files (no DB load)."""
    from motor_vehicles.scraping.marklines_parser import parse_page

    data_dir = Path("data/marklines")
    if not data_dir.exists():
        click.echo("No saved pages found. Run 'marklines download' first.")
        return

    total_sales = 0
    total_vtypes = 0
    total_commentary = 0
    for filepath in sorted(data_dir.glob("*.html")):
        html = filepath.read_text(encoding="utf-8")
        result = parse_page(html, source_url=filepath.name)
        n_sales = len(result.all_maker_sales)
        n_vtypes = len(result.all_vehicle_type_sales)
        n_commentary = len(result.all_commentary)
        total_sales += n_sales
        total_vtypes += n_vtypes
        total_commentary += n_commentary
        click.echo(
            f"  {filepath.name}: {n_sales} sales, "
            f"{n_vtypes} vehicle types, {n_commentary} commentary"
        )

    click.echo(
        f"\nTotal: {total_sales} sales, {total_vtypes} vehicle types, "
        f"{total_commentary} commentary records"
    )


def _marklines_full(config: AppConfig) -> None:
    """Full Marklines pipeline: fetch, parse, load to DB."""
    from motor_vehicles.scraping.marklines_client import MarklinesClient
    from motor_vehicles.scraping.marklines_parser import parse_page
    from motor_vehicles.storage.database import Database
    from motor_vehicles.storage.loader import load_marklines_data

    client = MarklinesClient(config.http, config.marklines)
    db = Database(config.database)
    try:
        db.connect()
        db.ensure_schema("migrations")
        run_id = db.start_run(source="marklines", config_hash=config.config_hash())

        # Check if we should use browser fallback
        use_browser = config.marklines.use_browser_fallback
        if use_browser:
            from motor_vehicles.scraping.marklines_browser import MarklinesBrowser

            browser = MarklinesBrowser()
            browser.start()
            urls = [config.marklines.base_url] + [
                f"{config.marklines.base_url}-{y}"
                for y in config.marklines.recent_years
            ] + [
                config.marklines.historical_url_template.format(year=y)
                for y in config.marklines.historical_years
            ]
            pages = browser.fetch_all_pages(urls)
            browser.close()
        else:
            pages = client.fetch_all_pages()

        all_sales: list[dict] = []
        all_vtypes: list[dict] = []
        all_commentary: list[dict] = []

        for url, html in pages.items():
            result = parse_page(html, source_url=url)
            all_sales.extend(result.all_maker_sales)
            all_vtypes.extend(result.all_vehicle_type_sales)
            all_commentary.extend(result.all_commentary)
            click.echo(
                f"  Parsed {url}: {len(result.all_maker_sales)} sales, "
                f"{len(result.all_vehicle_type_sales)} vtypes, "
                f"{len(result.all_commentary)} commentary"
            )

        total = load_marklines_data(
            db, run_id, all_sales, all_vtypes, all_commentary
        )
        db.finish_run(run_id, status="completed", records_count=total)
        click.echo(f"\nMarklines complete: {total} records loaded (run #{run_id})")

    except Exception as e:
        logger.error("Marklines pipeline failed: %s", e, exc_info=True)
        click.echo(f"Marklines pipeline failed: {e}")
        raise
    finally:
        client.close()
        db.close()


# ---------------------------------------------------------------------------
# Implementation: FCAI
# ---------------------------------------------------------------------------

def _fcai_download(
    config: AppConfig,
    year: int | None = None,
    month: str | None = None,
) -> None:
    """Download FCAI PDF publications."""
    from motor_vehicles.scraping.fcai_catalog import build_catalog
    from motor_vehicles.scraping.fcai_client import FcaiClient

    catalog = build_catalog(config.fcai, year=year, month=month)
    click.echo(f"Downloading {len(catalog)} FCAI publications...")

    client = FcaiClient(config.http, config.fcai)
    try:
        for entry in catalog:
            try:
                result = client.download_pdf(entry)
                if result["skipped"]:
                    click.echo(f"  [SKIP] {entry['filename']} (already exists)")
                else:
                    click.echo(
                        f"  [OK] {entry['filename']} "
                        f"({result['file_size_bytes']:,} bytes)"
                    )
            except Exception as e:
                click.echo(f"  [ERROR] {entry['filename']}: {e}")
    finally:
        client.close()


def _fcai_parse(config: AppConfig) -> None:
    """Extract data from downloaded FCAI PDFs (no DB load)."""
    from motor_vehicles.extraction.pdf_tables import extract_tables_from_pdf

    pdf_dir = Path(config.fcai.download_dir)
    if not pdf_dir.exists():
        click.echo("No PDFs found. Run 'fcai download' first.")
        return

    total_records = 0
    for filepath in sorted(pdf_dir.glob("*.pdf")):
        records = extract_tables_from_pdf(filepath)
        total_records += len(records)
        click.echo(f"  {filepath.name}: {len(records)} records")

    click.echo(f"\nTotal: {total_records} records extracted")


def _fcai_full(
    config: AppConfig,
    year: int | None = None,
    month: str | None = None,
) -> None:
    """Full FCAI pipeline: download, extract, load."""
    from motor_vehicles.extraction.pdf_tables import extract_tables_from_pdf
    from motor_vehicles.scraping.fcai_catalog import build_catalog
    from motor_vehicles.scraping.fcai_client import FcaiClient
    from motor_vehicles.storage.database import Database
    from motor_vehicles.storage.loader import load_fcai_publication

    catalog = build_catalog(config.fcai, year=year, month=month)

    client = FcaiClient(config.http, config.fcai)
    db = Database(config.database)
    try:
        db.connect()
        db.ensure_schema("migrations")
        run_id = db.start_run(source="fcai", config_hash=config.config_hash())

        total_records = 0
        for entry in catalog:
            try:
                result = client.download_pdf(entry)
                filepath = Path(result["filepath"])

                if not filepath.exists():
                    click.echo(f"  [SKIP] {entry['filename']} (download failed)")
                    continue

                # Extract tables from PDF
                records = extract_tables_from_pdf(filepath)
                if records:
                    publication = {
                        "year": entry["year"],
                        "month": entry["month_num"],
                        "filename": entry["filename"],
                        "url": entry["url"],
                        "file_hash": result.get("file_hash", ""),
                        "file_size_bytes": result.get("file_size_bytes", 0),
                    }
                    count = load_fcai_publication(db, run_id, publication, records)
                    total_records += count
                    click.echo(f"  [OK] {entry['filename']}: {count} records")
                else:
                    click.echo(f"  [EMPTY] {entry['filename']}: no tables extracted")

            except Exception as e:
                logger.error("Failed to process %s: %s", entry["filename"], e, exc_info=True)
                click.echo(f"  [ERROR] {entry['filename']}: {e}")

        db.finish_run(run_id, status="completed", records_count=total_records)
        click.echo(f"\nFCAI complete: {total_records} records loaded (run #{run_id})")

    except Exception as e:
        logger.error("FCAI pipeline failed: %s", e, exc_info=True)
        click.echo(f"FCAI pipeline failed: {e}")
        raise
    finally:
        client.close()
        db.close()


def _fcai_articles(
    config: AppConfig,
    url: str | None = None,
    list_only: bool = False,
    process_all: bool = False,
) -> None:
    """Scrape FCAI media release articles for table images."""
    from motor_vehicles.extraction.image_tables import (
        download_article_image,
        extract_tables_from_image,
    )
    from motor_vehicles.scraping.fcai_articles import (
        FcaiArticleScraper,
        classify_sales_article,
    )
    from motor_vehicles.storage.database import Database
    from motor_vehicles.storage.loader import load_fcai_article

    scraper = FcaiArticleScraper(config.http, config.fcai.articles)

    if list_only:
        click.echo("Fetching article listings...")
        listings = scraper.fetch_article_listings()
        sales_count = 0
        for listing in listings:
            is_sales = classify_sales_article(listing.title)
            marker = " *" if is_sales else "  "
            click.echo(f"{marker} {listing.date_text:20s} {listing.title}")
            click.echo(f"   {listing.url}")
            if is_sales:
                sales_count += 1
        click.echo(f"\nFound {len(listings)} articles ({sales_count} monthly sales articles marked with *)")
        scraper.close()
        return

    # Build list of URLs to process
    urls_to_process: list[str] = []
    if process_all:
        click.echo("Fetching article listings...")
        listings = scraper.fetch_article_listings()
        for listing in listings:
            if classify_sales_article(listing.title):
                urls_to_process.append(listing.url)
        click.echo(f"Found {len(urls_to_process)} monthly sales articles to process")
    elif url:
        urls_to_process = [url]
    else:
        click.echo("Please provide --url for a specific article, --list-only to browse, or --process-all.")
        scraper.close()
        return

    db = Database(config.database)
    try:
        db.connect()
        db.ensure_schema("migrations")
        run_id = db.start_run(source="fcai_articles", config_hash=config.config_hash())

        grand_total_images = 0
        grand_total_tables = 0

        for art_idx, art_url in enumerate(urls_to_process):
            click.echo(f"\n{'='*60}")
            click.echo(f"Article {art_idx + 1}/{len(urls_to_process)}: {art_url}")

            try:
                article = scraper.fetch_article(art_url)
            except Exception as e:
                logger.error("Failed to fetch article %s: %s", art_url, e, exc_info=True)
                click.echo(f"  [ERROR] Failed to fetch: {e}")
                continue

            click.echo(f"  Title: {article.title}")
            click.echo(f"  Year: {article.year}, Month: {article.month}")
            click.echo(f"  Images found: {len(article.image_urls)}")

            if not article.image_urls:
                click.echo("  No data images found, skipping.")
                continue

            # Download images and extract tables
            download_dir = config.fcai.articles.image_download_dir
            images: list[dict] = []
            extracted_tables: dict[int, list[dict]] = {}
            total_tables = 0

            for idx, img_url in enumerate(article.image_urls):
                label = article.image_labels[idx] if idx < len(article.image_labels) else ""
                click.echo(f"  Image {idx + 1}: {label or '(no label)'}")

                try:
                    filepath = download_article_image(scraper._client, img_url, download_dir)
                    filename = img_url.split("/")[-1].split("?")[0]

                    images.append({
                        "image_url": img_url,
                        "image_filename": filename,
                        "local_path": str(filepath),
                        "image_order": idx,
                        "image_label": label,
                    })

                    # Extract tables from image
                    tables = extract_tables_from_image(filepath, config.vision)
                    extracted_tables[idx] = tables
                    total_tables += len(tables)

                    for t_idx, table in enumerate(tables):
                        n_rows = len(table.get("rows", []))
                        n_cols = len(table.get("headers", []))
                        click.echo(f"    Table {t_idx + 1}: {n_cols} cols x {n_rows} rows")

                except Exception as e:
                    logger.error("Failed to process image %s: %s", img_url, e, exc_info=True)
                    click.echo(f"    [ERROR] {e}")

            # Load to database
            article_dict = {
                "url": article.url,
                "slug": article.slug,
                "title": article.title,
                "published_date": str(article.published_date) if article.published_date else None,
                "year": article.year,
                "month": article.month,
                "article_text": article.body_text,
                "is_sales_article": article.is_sales_article,
            }

            load_fcai_article(db, run_id, article_dict, images, extracted_tables)
            grand_total_images += len(images)
            grand_total_tables += total_tables
            click.echo(f"  Loaded: {len(images)} images, {total_tables} tables")

            # Rate limit between articles
            if art_idx < len(urls_to_process) - 1:
                scraper._delay()

        db.finish_run(run_id, status="completed", records_count=grand_total_tables)
        click.echo(
            f"\n{'='*60}"
            f"\nFCAI articles complete: {len(urls_to_process)} articles, "
            f"{grand_total_images} images, {grand_total_tables} tables (run #{run_id})"
        )

    except Exception as e:
        logger.error("FCAI articles pipeline failed: %s", e, exc_info=True)
        click.echo(f"FCAI articles pipeline failed: {e}")
        raise
    finally:
        scraper.close()
        db.close()


# ---------------------------------------------------------------------------
# Implementation: FCAI State/Territory
# ---------------------------------------------------------------------------

def _fcai_build_state_sales(config: AppConfig) -> None:
    """Extract State/Territory time-series from already-extracted article tables."""
    import json

    import pandas as pd

    from motor_vehicles.extraction.state_sales import extract_state_sales
    from motor_vehicles.storage.database import Database

    db = Database(config.database)
    try:
        db.connect()
        db.ensure_schema("migrations")

        with db.cursor() as cur:
            cur.execute('''
                SELECT t.id AS table_id, t.headers, t.row_data,
                       a.year, a.month
                FROM fcai_article_extracted_tables t
                JOIN fcai_article_images i ON t.image_id = i.id
                JOIN fcai_articles a ON i.article_id = a.id
                WHERE a.year IS NOT NULL AND a.month IS NOT NULL
                ORDER BY a.year, a.month
            ''')
            all_tables = cur.fetchall()

        click.echo(f"Scanning {len(all_tables)} extracted tables for State/Territory data...")

        total_records = 0
        months_found = set()

        for tbl in all_tables:
            headers = tbl["headers"] if isinstance(tbl["headers"], list) else json.loads(tbl["headers"])
            row_data = tbl["row_data"] if isinstance(tbl["row_data"], list) else json.loads(tbl["row_data"])

            records = extract_state_sales(headers, row_data, tbl["year"], tbl["month"])
            if not records:
                continue

            count = db.upsert_fcai_state_sales(records, source_table_id=tbl["table_id"])
            total_records += count
            key = (tbl["year"], tbl["month"])
            months_found.add(key)
            click.echo(
                f"  {tbl['year']}/{tbl['month']:02d}: "
                f"{count} state records (table #{tbl['table_id']})"
            )

        click.echo(f"\nLoaded {total_records} records across {len(months_found)} months")

        # Show coverage summary
        if months_found:
            sorted_months = sorted(months_found)
            first_y, first_m = sorted_months[0]
            last_y, last_m = sorted_months[-1]
            click.echo(f"Coverage: {first_y}/{first_m:02d} to {last_y}/{last_m:02d}")

            # Detect gaps
            all_expected = set()
            y, m = first_y, first_m
            while (y, m) <= (last_y, last_m):
                all_expected.add((y, m))
                m += 1
                if m > 12:
                    m = 1
                    y += 1
            gaps = sorted(all_expected - months_found)
            if gaps:
                gap_strs = [f"{y}/{m:02d}" for y, m in gaps]
                click.echo(f"Missing months: {', '.join(gap_strs)}")

        # Export CSV
        with db.cursor() as cur:
            cur.execute('''
                SELECT year, month, state_abbrev, state, units_sold,
                       units_sold_prev_year, yoy_pct
                FROM fcai_state_sales
                ORDER BY year, month, state_abbrev
            ''')
            rows = cur.fetchall()

        if rows:
            df = pd.DataFrame(rows)
            out_dir = Path(config.export.output_dir)
            out_dir.mkdir(parents=True, exist_ok=True)
            csv_path = out_dir / "fcai_state_sales.csv"
            df.to_csv(csv_path, index=False)
            click.echo(f"\nExported {len(df)} rows to {csv_path}")

    finally:
        db.close()


# ---------------------------------------------------------------------------
# Implementation: Top-level
# ---------------------------------------------------------------------------

def _run_full_pipeline(config: AppConfig) -> None:
    """Run both Marklines and FCAI pipelines."""
    click.echo("=== Motor Vehicles Scraper - Full Pipeline ===")
    click.echo(f"Mode: {config.run_mode}")
    click.echo(f"Database: {config.database.pg_host}:{config.database.pg_port}/{config.database.pg_database}")
    click.echo()

    click.echo("--- Marklines ---")
    try:
        _marklines_full(config)
    except Exception as e:
        click.echo(f"Marklines failed: {e}")

    click.echo()
    click.echo("--- FCAI ---")
    try:
        _fcai_full(config)
    except Exception as e:
        click.echo(f"FCAI failed: {e}")

    click.echo()
    click.echo("=== Pipeline complete ===")


def _run_migrate(config: AppConfig) -> None:
    """Run database migrations."""
    from motor_vehicles.storage.database import Database

    db = Database(config.database)
    try:
        db.connect()
        db.ensure_schema("migrations")
        click.echo("Migrations applied successfully")
    finally:
        db.close()


def _run_status(config: AppConfig) -> None:
    """Show recent run history and stats."""
    from motor_vehicles.storage.database import Database

    db = Database(config.database)
    try:
        db.connect()
        runs = db.get_run_history(limit=10)
        if not runs:
            click.echo("No scrape runs found. Run 'migrate' first, then 'run'.")
            return

        click.echo(f"{'ID':<6} {'Source':<12} {'Status':<12} {'Started':<22} {'Records':<10}")
        click.echo("-" * 70)
        for r in runs:
            started = str(r.get("started_at", ""))[:19]
            click.echo(
                f"{r['id']:<6} {r.get('source', ''):<12} {r.get('status', ''):<12} "
                f"{started:<22} {r.get('records_count', 0)!s:<10}"
            )

        stats = db.get_observation_stats()
        if stats:
            click.echo(f"\nDatabase totals:")
            click.echo(f"  Marklines sales:        {stats.get('marklines_sales_count', 0):,}")
            click.echo(f"  Marklines vehicle types: {stats.get('marklines_vtype_count', 0):,}")
            click.echo(f"  Marklines commentary:    {stats.get('marklines_commentary_count', 0):,}")
            click.echo(f"  FCAI publications:       {stats.get('fcai_publications_count', 0):,}")
            click.echo(f"  FCAI sales:              {stats.get('fcai_sales_count', 0):,}")
            click.echo(f"  FCAI articles:           {stats.get('fcai_articles_count', 0):,}")
            click.echo(f"  FCAI article images:     {stats.get('fcai_article_images_count', 0):,}")
            click.echo(f"  FCAI extracted tables:   {stats.get('fcai_extracted_tables_count', 0):,}")
    finally:
        db.close()


def _run_export(config: AppConfig, source: str = "all", fmt: str = "csv") -> None:
    """Export data to CSV."""
    import pandas as pd

    from motor_vehicles.storage.database import Database

    db = Database(config.database)
    try:
        db.connect()
        out_dir = Path(config.export.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S") if config.export.timestamp_files else ""

        if source in ("marklines", "all"):
            with db.cursor() as cur:
                cur.execute(
                    "SELECT year, month, make, units_sold, market_share, "
                    "units_sold_prev_year, yoy_pct, source_url "
                    "FROM marklines_sales ORDER BY year, month, make"
                )
                rows = cur.fetchall()
            if rows:
                df = pd.DataFrame(rows)
                fname = f"marklines_sales_{timestamp}.csv" if timestamp else "marklines_sales.csv"
                df.to_csv(out_dir / fname, index=False)
                click.echo(f"Exported {len(df)} marklines sales to {out_dir / fname}")

        if source in ("fcai", "all"):
            with db.cursor() as cur:
                cur.execute(
                    "SELECT year, month, make, model, segment, fuel_type, "
                    "units_sold, market_share FROM fcai_sales_data "
                    "ORDER BY year, month, make, model"
                )
                rows = cur.fetchall()
            if rows:
                df = pd.DataFrame(rows)
                fname = f"fcai_sales_{timestamp}.csv" if timestamp else "fcai_sales.csv"
                df.to_csv(out_dir / fname, index=False)
                click.echo(f"Exported {len(df)} FCAI sales to {out_dir / fname}")
    finally:
        db.close()


def _run_monthly_update(config: AppConfig) -> None:
    """Run monthly update and print report."""
    import json as json_mod

    from motor_vehicles.update import run_monthly_update

    click.echo("=== Monthly Update ===")
    report = run_monthly_update(config)
    click.echo(report.summary_text())

    # Write JSON report to exports
    out_dir = Path(config.export.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = out_dir / f"update_report_{ts}.json"
    json_path.write_text(
        json_mod.dumps(report.model_dump(), indent=2, default=str),
        encoding="utf-8",
    )
    click.echo(f"\nReport saved to {json_path}")
