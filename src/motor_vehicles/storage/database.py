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
                        (scrape_run_id, year, month, make, units_sold, source_url)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (year, month, make)
                    DO UPDATE SET
                        units_sold = EXCLUDED.units_sold,
                        source_url = EXCLUDED.source_url,
                        scrape_run_id = EXCLUDED.scrape_run_id
                    """,
                    (
                        run_id,
                        rec["year"],
                        rec["month"],
                        rec["make"],
                        rec.get("units_sold"),
                        rec.get("source_url", ""),
                    ),
                )
                count += 1
        logger.info("Upserted %d marklines sales records for run #%d", count, run_id)
        return count

    # --- Marklines Totals ---

    def upsert_marklines_totals(self, records: list[dict], run_id: int) -> int:
        """Bulk upsert marklines total records. Returns count."""
        if not records:
            return 0
        count = 0
        with self.cursor() as cur:
            for rec in records:
                cur.execute(
                    """
                    INSERT INTO marklines_totals
                        (scrape_run_id, year, month, total_units, source_url)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (year, month)
                    DO UPDATE SET
                        total_units = EXCLUDED.total_units,
                        source_url = EXCLUDED.source_url,
                        scrape_run_id = EXCLUDED.scrape_run_id
                    """,
                    (
                        run_id,
                        rec["year"],
                        rec["month"],
                        rec.get("total_units"),
                        rec.get("source_url", ""),
                    ),
                )
                count += 1
        logger.info("Upserted %d marklines totals for run #%d", count, run_id)
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

    # --- Stats ---

    def get_observation_stats(self) -> dict:
        """Get summary statistics about loaded data."""
        with self.cursor() as cur:
            cur.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM marklines_sales) as marklines_sales_count,
                    (SELECT COUNT(*) FROM marklines_totals) as marklines_totals_count,
                    (SELECT COUNT(*) FROM fcai_publications) as fcai_publications_count,
                    (SELECT COUNT(*) FROM fcai_sales_data) as fcai_sales_count
                """
            )
            return cur.fetchone() or {}
