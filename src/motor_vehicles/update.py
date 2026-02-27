"""
Monthly update orchestrator.

Pure Python functions returning Pydantic report models -- no Click dependency.
Designed to be called from the CLI or directly from a future Prefect flow.

Usage:
    from motor_vehicles.update import run_monthly_update
    report = run_monthly_update(config)
    print(report.summary_text())
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from datetime import date, datetime
from pathlib import Path

from pydantic import BaseModel, Field

from motor_vehicles.config import AppConfig

logger = logging.getLogger("motor_vehicles.update")


# ---------------------------------------------------------------------------
# Report models
# ---------------------------------------------------------------------------

class StepError(BaseModel):
    """An error that occurred during one step of the update."""
    source: str
    message: str
    detail: str = ""


class MarklinesStepReport(BaseModel):
    """Results from the Marklines update step."""
    pages_fetched: int = 0
    sales_records: int = 0
    vehicle_type_records: int = 0
    commentary_records: int = 0
    total_records: int = 0
    skipped_unchanged: bool = False
    errors: list[StepError] = Field(default_factory=list)


class FcaiArticlesStepReport(BaseModel):
    """Results from the FCAI articles update step."""
    articles_found: int = 0
    articles_already_processed: int = 0
    articles_new: int = 0
    images_processed: int = 0
    tables_extracted: int = 0
    errors: list[StepError] = Field(default_factory=list)


class StateSalesStepReport(BaseModel):
    """Results from the State/Territory sales extraction step."""
    tables_scanned: int = 0
    months_found: int = 0
    records_upserted: int = 0
    coverage_gaps: list[str] = Field(default_factory=list)


class UpdateReport(BaseModel):
    """Complete report from a monthly update run."""
    timestamp: str
    marklines: MarklinesStepReport | None = None
    fcai_articles: FcaiArticlesStepReport | None = None
    state_sales: StateSalesStepReport | None = None
    quality_issues: list[dict] = Field(default_factory=list)
    errors: list[StepError] = Field(default_factory=list)
    duration_seconds: float = 0.0

    def summary_text(self) -> str:
        """Human-readable summary for console or Slack output."""
        lines = [
            f"=== Monthly Update Report ({self.timestamp}) ===",
            f"Duration: {self.duration_seconds:.1f}s",
            "",
        ]

        if self.marklines:
            m = self.marklines
            lines.append("--- Marklines ---")
            if m.skipped_unchanged:
                lines.append(f"  Pages fetched:    {m.pages_fetched}")
                lines.append("  Content unchanged, skipped parse/load")
            else:
                lines.append(f"  Pages fetched:    {m.pages_fetched}")
                lines.append(f"  Sales records:    {m.sales_records}")
                lines.append(f"  Vehicle types:    {m.vehicle_type_records}")
                lines.append(f"  Commentary:       {m.commentary_records}")
                lines.append(f"  Total loaded:     {m.total_records}")
            if m.errors:
                for err in m.errors:
                    lines.append(f"  ERROR: {err.message}")
            lines.append("")

        if self.fcai_articles:
            f = self.fcai_articles
            lines.append("--- FCAI Articles ---")
            lines.append(f"  Articles found:   {f.articles_found}")
            lines.append(f"  Already processed:{f.articles_already_processed}")
            lines.append(f"  New articles:     {f.articles_new}")
            lines.append(f"  Images processed: {f.images_processed}")
            lines.append(f"  Tables extracted: {f.tables_extracted}")
            if f.errors:
                for err in f.errors:
                    lines.append(f"  ERROR: {err.message}")
            lines.append("")

        if self.state_sales:
            s = self.state_sales
            lines.append("--- State/Territory Sales ---")
            lines.append(f"  Tables scanned:   {s.tables_scanned}")
            lines.append(f"  Months found:     {s.months_found}")
            lines.append(f"  Records upserted: {s.records_upserted}")
            if s.coverage_gaps:
                lines.append(f"  Coverage gaps:    {', '.join(s.coverage_gaps)}")
            lines.append("")

        if self.quality_issues:
            lines.append("--- Quality Checks ---")
            for issue in self.quality_issues:
                icon = "ERROR" if issue.get("severity") == "error" else "WARN"
                lines.append(f"  [{icon}] {issue['check']}: {issue['message']}")
            lines.append("")

        if self.errors:
            lines.append("--- Top-level Errors ---")
            for err in self.errors:
                lines.append(f"  [{err.source}] {err.message}")
            lines.append("")

        total_errors = (
            len(self.errors)
            + (len(self.marklines.errors) if self.marklines else 0)
            + (len(self.fcai_articles.errors) if self.fcai_articles else 0)
        )
        status = "CLEAN" if total_errors == 0 else f"{total_errors} error(s)"
        lines.append(f"Status: {status}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def compute_marklines_years(today: date | None = None) -> list[int]:
    """Return years to fetch as separate year pages.

    The current page already covers the current year, so we only need
    the previous year as an additional page fetch.
    """
    if today is None:
        today = date.today()
    return [today.year - 1]


# ---------------------------------------------------------------------------
# Step 1: Marklines
# ---------------------------------------------------------------------------

def _hash_pages(pages: dict[str, str]) -> str:
    """Compute a deterministic hash of fetched page content."""
    h = hashlib.sha256()
    for url in sorted(pages):
        h.update(url.encode())
        h.update(pages[url].encode())
    return h.hexdigest()


def run_marklines_update(config: AppConfig) -> MarklinesStepReport:
    """Fetch current + previous year Marklines pages, parse, and load."""
    from motor_vehicles.scraping.marklines_client import MarklinesClient
    from motor_vehicles.scraping.marklines_parser import parse_page
    from motor_vehicles.storage.database import Database
    from motor_vehicles.storage.loader import load_marklines_data

    report = MarklinesStepReport()
    years = compute_marklines_years()
    logger.info("Marklines update: fetching years %s", years)

    client = MarklinesClient(config.http, config.marklines)
    db = Database(config.database)
    try:
        db.connect()
        db.ensure_schema("migrations")

        pages: dict[str, str] = {}

        # Fetch current page
        try:
            url = config.marklines.base_url
            pages[url] = client.fetch_current_page()
        except Exception as e:
            logger.error("Failed to fetch current Marklines page: %s", e, exc_info=True)
            report.errors.append(StepError(
                source="marklines", message=f"Failed to fetch current page: {e}",
            ))

        # Fetch year pages
        for year in years:
            try:
                url = client._build_url(year)
                if url not in pages:
                    pages[url] = client.fetch_year_page(year)
            except Exception as e:
                logger.error("Failed to fetch Marklines year %d: %s", year, e, exc_info=True)
                report.errors.append(StepError(
                    source="marklines", message=f"Failed to fetch year {year}: {e}",
                ))

        report.pages_fetched = len(pages)

        # Incremental check: skip if content unchanged since last run
        content_hash = _hash_pages(pages) if pages else ""
        previous_hash = db.get_last_content_hash("marklines_update")
        if content_hash and content_hash == previous_hash:
            logger.info("Marklines content unchanged (hash=%s), skipping parse/load", content_hash[:12])
            report.skipped_unchanged = True
            return report

        run_id = db.start_run(source="marklines_update", config_hash=config.config_hash())

        # Parse all fetched pages
        all_sales: list[dict] = []
        all_vtypes: list[dict] = []
        all_commentary: list[dict] = []

        for url, html in pages.items():
            try:
                result = parse_page(html, source_url=url)
                all_sales.extend(result.all_maker_sales)
                all_vtypes.extend(result.all_vehicle_type_sales)
                all_commentary.extend(result.all_commentary)
            except Exception as e:
                logger.error("Failed to parse Marklines page %s: %s", url, e, exc_info=True)
                report.errors.append(StepError(
                    source="marklines", message=f"Failed to parse {url}: {e}",
                ))

        # Load to database
        try:
            total = load_marklines_data(db, run_id, all_sales, all_vtypes, all_commentary)
            report.sales_records = len(all_sales)
            report.vehicle_type_records = len(all_vtypes)
            report.commentary_records = len(all_commentary)
            report.total_records = total
            db.finish_run(run_id, status="completed", records_count=total,
                          content_hash=content_hash)
        except Exception as e:
            logger.error("Failed to load Marklines data: %s", e, exc_info=True)
            report.errors.append(StepError(
                source="marklines", message=f"Failed to load data: {e}",
            ))
            db.finish_run(run_id, status="failed", error_message=str(e))

    except Exception as e:
        logger.error("Marklines update failed: %s", e, exc_info=True)
        report.errors.append(StepError(
            source="marklines", message=f"Step failed: {e}",
        ))
    finally:
        client.close()
        db.close()

    return report


# ---------------------------------------------------------------------------
# Step 2: FCAI Articles
# ---------------------------------------------------------------------------

def run_fcai_articles_update(
    config: AppConfig,
    max_pages: int | None = None,
) -> FcaiArticlesStepReport:
    """Fetch article listings, skip already-processed, process new articles."""
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

    report = FcaiArticlesStepReport()

    scraper = FcaiArticleScraper(config.http, config.fcai.articles)
    db = Database(config.database)
    try:
        # Fetch listings
        listings = scraper.fetch_article_listings(max_pages=max_pages)
        sales_urls = [
            listing.url for listing in listings
            if classify_sales_article(listing.title)
        ]
        report.articles_found = len(sales_urls)
        logger.info("FCAI articles update: found %d sales articles", len(sales_urls))

        # Connect to DB and check which URLs are already processed
        db.connect()
        db.ensure_schema("migrations")
        existing_urls = db.get_existing_article_urls()

        new_urls = [u for u in sales_urls if u not in existing_urls]
        report.articles_already_processed = len(sales_urls) - len(new_urls)
        report.articles_new = len(new_urls)
        logger.info(
            "FCAI articles: %d new, %d already processed",
            len(new_urls), report.articles_already_processed,
        )

        if not new_urls:
            return report

        run_id = db.start_run(source="fcai_articles_update", config_hash=config.config_hash())

        for art_idx, art_url in enumerate(new_urls):
            try:
                article = scraper.fetch_article(art_url)
            except Exception as e:
                logger.error("Failed to fetch article %s: %s", art_url, e, exc_info=True)
                report.errors.append(StepError(
                    source="fcai_articles", message=f"Failed to fetch {art_url}: {e}",
                ))
                continue

            images: list[dict] = []
            extracted_tables: dict[int, list[dict]] = {}
            article_tables = 0

            if article.image_urls:
                # Primary path: download images and extract tables via Vision LLM
                download_dir = config.fcai.articles.image_download_dir

                for idx, img_url in enumerate(article.image_urls):
                    label = article.image_labels[idx] if idx < len(article.image_labels) else ""
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

                        tables = extract_tables_from_image(filepath, config.vision)
                        extracted_tables[idx] = tables
                        article_tables += len(tables)

                    except Exception as e:
                        logger.error("Failed to process image %s: %s", img_url, e, exc_info=True)
                        report.errors.append(StepError(
                            source="fcai_articles",
                            message=f"Image processing failed: {e}",
                            detail=img_url,
                        ))
            elif article.html_tables:
                # Fallback: extract structured data from HTML tables in article body
                logger.info(
                    "Article %s has no images but %d HTML tables, using fallback",
                    art_url, len(article.html_tables),
                )
                for idx, html_table in enumerate(article.html_tables):
                    if html_table.get("rows"):
                        extracted_tables[idx] = [{
                            "headers": html_table["headers"],
                            "rows": html_table["rows"],
                            "dataframe_csv": "",
                            "table_index": idx,
                            "extraction_method": "html_table",
                            "confidence": 0.95,
                        }]
                        article_tables += 1
            else:
                logger.info("Article %s has no images or HTML tables, skipping", art_url)
                continue

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
            report.images_processed += len(images)
            report.tables_extracted += article_tables

            # Rate limit between articles
            if art_idx < len(new_urls) - 1:
                scraper._delay()

        db.finish_run(run_id, status="completed", records_count=report.tables_extracted)

    except Exception as e:
        logger.error("FCAI articles update failed: %s", e, exc_info=True)
        report.errors.append(StepError(
            source="fcai_articles", message=f"Step failed: {e}",
        ))
    finally:
        scraper.close()
        db.close()

    return report


# ---------------------------------------------------------------------------
# Step 3: State/Territory Sales
# ---------------------------------------------------------------------------

def run_state_sales_update(config: AppConfig) -> StateSalesStepReport:
    """Re-extract state sales from all article tables (idempotent via upsert)."""
    from motor_vehicles.extraction.state_sales import extract_state_sales
    from motor_vehicles.storage.database import Database

    report = StateSalesStepReport()

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

        report.tables_scanned = len(all_tables)
        logger.info("State sales update: scanning %d extracted tables", len(all_tables))

        months_found: set[tuple[int, int]] = set()

        for tbl in all_tables:
            headers = tbl["headers"] if isinstance(tbl["headers"], list) else json.loads(tbl["headers"])
            row_data = tbl["row_data"] if isinstance(tbl["row_data"], list) else json.loads(tbl["row_data"])

            records = extract_state_sales(headers, row_data, tbl["year"], tbl["month"])
            if not records:
                continue

            count = db.upsert_fcai_state_sales(records, source_table_id=tbl["table_id"])
            report.records_upserted += count
            months_found.add((tbl["year"], tbl["month"]))

        report.months_found = len(months_found)

        # Detect coverage gaps
        if months_found:
            sorted_months = sorted(months_found)
            first_y, first_m = sorted_months[0]
            last_y, last_m = sorted_months[-1]

            all_expected: set[tuple[int, int]] = set()
            y, m = first_y, first_m
            while (y, m) <= (last_y, last_m):
                all_expected.add((y, m))
                m += 1
                if m > 12:
                    m = 1
                    y += 1
            gaps = sorted(all_expected - months_found)
            report.coverage_gaps = [f"{y}/{m:02d}" for y, m in gaps]

    except Exception as e:
        logger.error("State sales update failed: %s", e, exc_info=True)
        # No errors list on this model, propagate up
        raise
    finally:
        db.close()

    return report


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_monthly_update(
    config: AppConfig,
    max_pages: int | None = None,
) -> UpdateReport:
    """Run all three update steps and return a complete report."""
    start = time.monotonic()
    report = UpdateReport(timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # Step 1: Marklines
    logger.info("=== Step 1: Marklines ===")
    try:
        report.marklines = run_marklines_update(config)
    except Exception as e:
        logger.error("Marklines step failed: %s", e, exc_info=True)
        report.errors.append(StepError(
            source="marklines", message=f"Step failed: {e}",
        ))

    # Step 2: FCAI Articles
    logger.info("=== Step 2: FCAI Articles ===")
    try:
        report.fcai_articles = run_fcai_articles_update(config, max_pages=max_pages)
    except Exception as e:
        logger.error("FCAI articles step failed: %s", e, exc_info=True)
        report.errors.append(StepError(
            source="fcai_articles", message=f"Step failed: {e}",
        ))

    # Step 3: State Sales
    logger.info("=== Step 3: State/Territory Sales ===")
    try:
        report.state_sales = run_state_sales_update(config)
    except Exception as e:
        logger.error("State sales step failed: %s", e, exc_info=True)
        report.errors.append(StepError(
            source="state_sales", message=f"Step failed: {e}",
        ))

    # Step 4: Quality Checks
    logger.info("=== Step 4: Quality Checks ===")
    try:
        report.quality_issues = _run_quality_checks(config)
    except Exception as e:
        logger.error("Quality checks failed: %s", e, exc_info=True)
        # Non-fatal: don't append to errors, just log

    report.duration_seconds = round(time.monotonic() - start, 2)
    return report


def _run_quality_checks(config: AppConfig) -> list[dict]:
    """Run quality checks and return serializable issue list."""
    from motor_vehicles.quality import run_quality_checks
    from motor_vehicles.storage.database import Database

    db = Database(config.database)
    try:
        db.connect()
        qr = run_quality_checks(db)
        return [issue.model_dump() for issue in qr.issues]
    finally:
        db.close()
