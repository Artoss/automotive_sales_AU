"""
PostgreSQL storage layer.
Handles connections, migrations, and CRUD operations.
Uses psycopg 3 with dict_row factory.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import psycopg
from psycopg.rows import dict_row

from motor_vehicles.config import DatabaseConfig

logger = logging.getLogger("motor_vehicles.storage.database")


class Database:
    """PostgreSQL database interface for the motor vehicles scraper."""

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._conn: psycopg.Connection | None = None

    # --- Connection Management ---

    def connect(self) -> None:
        logger.info(
            "Connecting to PostgreSQL at %s:%s/%s",
            self.config.pg_host,
            self.config.pg_port,
            self.config.pg_database,
        )
        self._conn = psycopg.connect(
            **self.config.connection_params,
            row_factory=dict_row,
            autocommit=False,
        )
        logger.info("Connected successfully")

    def close(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()
            logger.info("Database connection closed")

    @contextmanager
    def cursor(self) -> Generator[psycopg.Cursor, None, None]:
        assert self._conn is not None, "Database not connected"
        with self._conn.cursor() as cur:
            try:
                yield cur
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    # --- Schema / Migrations ---

    def run_migration(self, migration_path: str | Path) -> None:
        path = Path(migration_path)
        sql = path.read_text(encoding="utf-8")
        logger.info("Running migration: %s", path.name)
        with self.cursor() as cur:
            cur.execute(psycopg.sql.SQL(sql))

    def ensure_schema(self, migrations_dir: str | Path = "migrations") -> None:
        mdir = Path(migrations_dir)
        if not mdir.exists():
            logger.warning("Migrations directory not found: %s", mdir)
            return
        for mf in sorted(mdir.glob("*.sql")):
            self.run_migration(mf)
        logger.info("All migrations applied")

    # --- Scrape Runs ---

    def start_run(self, source: str, config_hash: str = "") -> int:
        with self.cursor() as cur:
            cur.execute(
                """
                INSERT INTO scrape_runs (source, status, error_message)
                VALUES (%s, 'running', %s)
                RETURNING id
                """,
                (source, config_hash),
            )
            row = cur.fetchone()
            assert row is not None
            run_id = row["id"]
            logger.info("Started scrape run #%d (source=%s)", run_id, source)
            return run_id

    def finish_run(
        self,
        run_id: int,
        status: str = "completed",
        records_count: int = 0,
        error_message: str | None = None,
    ) -> None:
        with self.cursor() as cur:
            cur.execute(
                """
                UPDATE scrape_runs
                SET completed_at = NOW(),
                    status = %s,
                    records_count = %s,
                    error_message = COALESCE(%s, error_message)
                WHERE id = %s
                """,
                (status, records_count, error_message, run_id),
            )
        logger.info("Finished scrape run #%d (status=%s)", run_id, status)

    def get_run_history(self, limit: int = 10) -> list[dict]:
        with self.cursor() as cur:
            cur.execute(
                "SELECT * FROM scrape_runs ORDER BY started_at DESC LIMIT %s",
                (limit,),
            )
            return cur.fetchall()

    # --- Marklines Sales ---

    def upsert_marklines_sales(self, records: list[dict], run_id: int) -> int:
        """Bulk upsert marklines sales records. Returns count."""
        if not records:
            return 0
        count = 0
        with self.cursor() as cur:
            for rec in records:
                cur.execute(
                    """
                    INSERT INTO marklines_sales
                        (scrape_run_id, year, month, make, units_sold,
                         market_share, units_sold_prev_year, yoy_pct, source_url)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (year, month, make)
                    DO UPDATE SET
                        units_sold = EXCLUDED.units_sold,
                        market_share = EXCLUDED.market_share,
                        units_sold_prev_year = EXCLUDED.units_sold_prev_year,
                        yoy_pct = EXCLUDED.yoy_pct,
                        source_url = EXCLUDED.source_url,
                        scrape_run_id = EXCLUDED.scrape_run_id
                    """,
                    (
                        run_id,
                        rec["year"],
                        rec["month"],
                        rec["make"],
                        rec.get("units_sold"),
                        rec.get("market_share"),
                        rec.get("units_sold_prev_year"),
                        rec.get("yoy_pct"),
                        rec.get("source_url", ""),
                    ),
                )
                count += 1
        logger.info("Upserted %d marklines sales records for run #%d", count, run_id)
        return count

    # --- Marklines Vehicle Type Sales ---

    def upsert_marklines_vehicle_types(
        self, records: list[dict], run_id: int
    ) -> int:
        """Bulk upsert marklines vehicle type sales. Returns count."""
        if not records:
            return 0
        count = 0
        with self.cursor() as cur:
            for rec in records:
                cur.execute(
                    """
                    INSERT INTO marklines_vehicle_type_sales
                        (scrape_run_id, year, month, vehicle_type,
                         units_sold, units_sold_prev_year, yoy_pct, source_url)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (year, month, vehicle_type)
                    DO UPDATE SET
                        units_sold = EXCLUDED.units_sold,
                        units_sold_prev_year = EXCLUDED.units_sold_prev_year,
                        yoy_pct = EXCLUDED.yoy_pct,
                        source_url = EXCLUDED.source_url,
                        scrape_run_id = EXCLUDED.scrape_run_id
                    """,
                    (
                        run_id,
                        rec["year"],
                        rec["month"],
                        rec["vehicle_type"],
                        rec.get("units_sold"),
                        rec.get("units_sold_prev_year"),
                        rec.get("yoy_pct"),
                        rec.get("source_url", ""),
                    ),
                )
                count += 1
        logger.info(
            "Upserted %d marklines vehicle type records for run #%d", count, run_id
        )
        return count

    # --- Marklines Commentary ---

    def upsert_marklines_commentary(
        self, records: list[dict], run_id: int
    ) -> int:
        """Bulk upsert marklines commentary records. Returns count."""
        if not records:
            return 0
        count = 0
        with self.cursor() as cur:
            for rec in records:
                cur.execute(
                    """
                    INSERT INTO marklines_commentary
                        (scrape_run_id, year, month, report_date,
                         commentary, source_url)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (year, month)
                    DO UPDATE SET
                        report_date = EXCLUDED.report_date,
                        commentary = EXCLUDED.commentary,
                        source_url = EXCLUDED.source_url,
                        scrape_run_id = EXCLUDED.scrape_run_id
                    """,
                    (
                        run_id,
                        rec["year"],
                        rec["month"],
                        rec.get("report_date", ""),
                        rec["commentary"],
                        rec.get("source_url", ""),
                    ),
                )
                count += 1
        logger.info(
            "Upserted %d marklines commentary records for run #%d", count, run_id
        )
        return count

    # --- FCAI Publications ---

    def upsert_fcai_publication(self, record: dict, run_id: int) -> int:
        """Upsert an FCAI publication record. Returns the publication id."""
        with self.cursor() as cur:
            cur.execute(
                """
                INSERT INTO fcai_publications
                    (scrape_run_id, year, month, filename, url,
                     file_hash, file_size_bytes, downloaded_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (year, month, filename)
                DO UPDATE SET
                    file_hash = EXCLUDED.file_hash,
                    file_size_bytes = EXCLUDED.file_size_bytes,
                    downloaded_at = EXCLUDED.downloaded_at,
                    scrape_run_id = EXCLUDED.scrape_run_id
                RETURNING id
                """,
                (
                    run_id,
                    record["year"],
                    record["month"],
                    record["filename"],
                    record["url"],
                    record.get("file_hash", ""),
                    record.get("file_size_bytes", 0),
                ),
            )
            row = cur.fetchone()
            assert row is not None
            return row["id"]

    def get_publication_hash(self, filename: str) -> str | None:
        """Get the most recent file hash for incremental mode."""
        with self.cursor() as cur:
            cur.execute(
                """
                SELECT file_hash FROM fcai_publications
                WHERE filename = %s
                ORDER BY downloaded_at DESC
                LIMIT 1
                """,
                (filename,),
            )
            row = cur.fetchone()
            return row["file_hash"] if row else None

    def mark_publication_parsed(self, publication_id: int) -> None:
        with self.cursor() as cur:
            cur.execute(
                "UPDATE fcai_publications SET parsed = TRUE WHERE id = %s",
                (publication_id,),
            )

    # --- FCAI Sales Data ---

    def upsert_fcai_sales(self, records: list[dict], publication_id: int) -> int:
        """Bulk upsert FCAI sales data records. Returns count."""
        if not records:
            return 0
        count = 0
        with self.cursor() as cur:
            for rec in records:
                cur.execute(
                    """
                    INSERT INTO fcai_sales_data
                        (publication_id, year, month, make, model,
                         segment, fuel_type, units_sold, market_share)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (year, month, make, model, segment)
                    DO UPDATE SET
                        fuel_type = EXCLUDED.fuel_type,
                        units_sold = EXCLUDED.units_sold,
                        market_share = EXCLUDED.market_share,
                        publication_id = EXCLUDED.publication_id
                    """,
                    (
                        publication_id,
                        rec["year"],
                        rec["month"],
                        rec.get("make", ""),
                        rec.get("model", ""),
                        rec.get("segment", ""),
                        rec.get("fuel_type", ""),
                        rec.get("units_sold"),
                        rec.get("market_share"),
                    ),
                )
                count += 1
        logger.info(
            "Upserted %d FCAI sales records for publication #%d", count, publication_id
        )
        return count

    # --- FCAI Articles ---

    def upsert_fcai_article(self, record: dict, run_id: int) -> int:
        """Upsert an FCAI article record. Returns the article id."""
        with self.cursor() as cur:
            cur.execute(
                """
                INSERT INTO fcai_articles
                    (scrape_run_id, url, slug, title, published_date,
                     year, month, article_text, is_sales_article)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (url)
                DO UPDATE SET
                    title = EXCLUDED.title,
                    published_date = EXCLUDED.published_date,
                    year = EXCLUDED.year,
                    month = EXCLUDED.month,
                    article_text = EXCLUDED.article_text,
                    is_sales_article = EXCLUDED.is_sales_article,
                    scrape_run_id = EXCLUDED.scrape_run_id,
                    scraped_at = NOW()
                RETURNING id
                """,
                (
                    run_id,
                    record["url"],
                    record["slug"],
                    record["title"],
                    record.get("published_date"),
                    record.get("year"),
                    record.get("month"),
                    record.get("article_text", ""),
                    record.get("is_sales_article", False),
                ),
            )
            row = cur.fetchone()
            assert row is not None
            article_id = row["id"]
            logger.info("Upserted FCAI article #%d: %s", article_id, record["slug"])
            return article_id

    def upsert_fcai_article_image(self, article_id: int, record: dict) -> int:
        """Upsert an FCAI article image record. Returns the image id."""
        with self.cursor() as cur:
            cur.execute(
                """
                INSERT INTO fcai_article_images
                    (article_id, image_url, image_filename, local_path,
                     image_order, image_label, width, height)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (article_id, image_url)
                DO UPDATE SET
                    local_path = EXCLUDED.local_path,
                    image_order = EXCLUDED.image_order,
                    image_label = EXCLUDED.image_label,
                    width = EXCLUDED.width,
                    height = EXCLUDED.height,
                    downloaded_at = NOW()
                RETURNING id
                """,
                (
                    article_id,
                    record["image_url"],
                    record["image_filename"],
                    record.get("local_path", ""),
                    record.get("image_order", 0),
                    record.get("image_label", ""),
                    record.get("width"),
                    record.get("height"),
                ),
            )
            row = cur.fetchone()
            assert row is not None
            return row["id"]

    def insert_fcai_extracted_table(self, image_id: int, record: dict) -> int:
        """Upsert an extracted table record. Returns the table id."""
        import json

        with self.cursor() as cur:
            cur.execute(
                """
                INSERT INTO fcai_article_extracted_tables
                    (image_id, table_index, headers, row_data,
                     dataframe_csv, extraction_method, confidence)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (image_id, table_index) DO UPDATE SET
                    headers = EXCLUDED.headers,
                    row_data = EXCLUDED.row_data,
                    dataframe_csv = EXCLUDED.dataframe_csv,
                    extraction_method = EXCLUDED.extraction_method,
                    confidence = EXCLUDED.confidence,
                    extracted_at = NOW()
                RETURNING id
                """,
                (
                    image_id,
                    record.get("table_index", 0),
                    json.dumps(record.get("headers", [])),
                    json.dumps(record.get("rows", [])),
                    record.get("dataframe_csv", ""),
                    record.get("extraction_method", "vision_llm"),
                    record.get("confidence", 0.85),
                ),
            )
            row = cur.fetchone()
            assert row is not None
            return row["id"]

    # --- Stats ---

    def get_observation_stats(self) -> dict:
        """Get summary statistics about loaded data."""
        with self.cursor() as cur:
            cur.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM marklines_sales) as marklines_sales_count,
                    (SELECT COUNT(*) FROM marklines_vehicle_type_sales) as marklines_vtype_count,
                    (SELECT COUNT(*) FROM marklines_commentary) as marklines_commentary_count,
                    (SELECT COUNT(*) FROM fcai_publications) as fcai_publications_count,
                    (SELECT COUNT(*) FROM fcai_sales_data) as fcai_sales_count,
                    (SELECT COUNT(*) FROM fcai_articles) as fcai_articles_count,
                    (SELECT COUNT(*) FROM fcai_article_images) as fcai_article_images_count,
                    (SELECT COUNT(*) FROM fcai_article_extracted_tables) as fcai_extracted_tables_count
                """
            )
            return cur.fetchone() or {}
