"""
Integration tests against the live PostgreSQL database.

Requires: automotive_sales_au database with migrations applied.
Run: uv run pytest tests/test_database_live.py -v
"""

from __future__ import annotations

import pytest

from motor_vehicles.config import DatabaseConfig
from motor_vehicles.storage.database import Database


@pytest.fixture
def db():
    """Connect to the live database, yield, then close."""
    config = DatabaseConfig(pg_database="automotive_sales_au")
    database = Database(config)
    database.connect()
    yield database
    database.close()


class TestScrapeRuns:
    """Test scrape_runs CRUD."""

    def test_start_and_finish_run(self, db: Database):
        run_id = db.start_run(source="test", config_hash="abc123")
        assert run_id > 0

        db.finish_run(run_id, status="completed", records_count=42)

        history = db.get_run_history(limit=1)
        assert len(history) >= 1
        latest = history[0]
        assert latest["id"] == run_id
        assert latest["source"] == "test"
        assert latest["status"] == "completed"
        assert latest["records_count"] == 42

    def test_failed_run(self, db: Database):
        run_id = db.start_run(source="test")
        db.finish_run(run_id, status="failed", error_message="something broke")

        history = db.get_run_history(limit=1)
        assert history[0]["status"] == "failed"


class TestMarklinesSales:
    """Test marklines_sales upserts."""

    def test_upsert_sales(self, db: Database):
        run_id = db.start_run(source="test_marklines")

        records = [
            {"year": 9999, "month": 1, "make": "TestMake", "units_sold": 100, "source_url": "http://test"},
            {"year": 9999, "month": 2, "make": "TestMake", "units_sold": 200, "source_url": "http://test"},
        ]
        count = db.upsert_marklines_sales(records, run_id)
        assert count == 2

        # Upsert again with updated values
        records[0]["units_sold"] = 150
        count = db.upsert_marklines_sales(records, run_id)
        assert count == 2

        # Verify the update took effect
        with db.cursor() as cur:
            cur.execute(
                "SELECT units_sold FROM marklines_sales WHERE year = 9999 AND month = 1 AND make = 'TestMake'"
            )
            row = cur.fetchone()
            assert row["units_sold"] == 150

        db.finish_run(run_id, status="completed", records_count=count)


class TestMarklinesTotals:
    """Test marklines_totals upserts."""

    def test_upsert_totals(self, db: Database):
        run_id = db.start_run(source="test_totals")

        records = [
            {"year": 9999, "month": 1, "total_units": 50000, "source_url": "http://test"},
            {"year": 9999, "month": 2, "total_units": 48000, "source_url": "http://test"},
        ]
        count = db.upsert_marklines_totals(records, run_id)
        assert count == 2

        db.finish_run(run_id, status="completed", records_count=count)


class TestFcaiPublications:
    """Test fcai_publications upserts."""

    def test_upsert_publication(self, db: Database):
        run_id = db.start_run(source="test_fcai")

        pub = {
            "year": 9999,
            "month": 1,
            "filename": "test_9999_vfacts.pdf",
            "url": "http://test/test_9999_vfacts.pdf",
            "file_hash": "abc123hash",
            "file_size_bytes": 12345,
        }
        pub_id = db.upsert_fcai_publication(pub, run_id)
        assert pub_id > 0

        # Upsert again should return same id
        pub_id_2 = db.upsert_fcai_publication(pub, run_id)
        assert pub_id_2 == pub_id

        db.finish_run(run_id, status="completed")

    def test_get_publication_hash(self, db: Database):
        run_id = db.start_run(source="test_hash")

        pub = {
            "year": 9998,
            "month": 6,
            "filename": "hash_test_9998.pdf",
            "url": "http://test/hash_test.pdf",
            "file_hash": "deadbeef",
            "file_size_bytes": 999,
        }
        db.upsert_fcai_publication(pub, run_id)

        result = db.get_publication_hash("hash_test_9998.pdf")
        assert result == "deadbeef"

        result_none = db.get_publication_hash("nonexistent.pdf")
        assert result_none is None

        db.finish_run(run_id, status="completed")


class TestFcaiSalesData:
    """Test fcai_sales_data upserts."""

    def test_upsert_sales_data(self, db: Database):
        run_id = db.start_run(source="test_fcai_sales")

        pub = {
            "year": 9997,
            "month": 3,
            "filename": "sales_test_9997.pdf",
            "url": "http://test/sales.pdf",
            "file_hash": "abc",
            "file_size_bytes": 100,
        }
        pub_id = db.upsert_fcai_publication(pub, run_id)

        records = [
            {"year": 9997, "month": 3, "make": "TestBrand", "model": "ModelA",
             "segment": "SUV", "fuel_type": "Petrol", "units_sold": 500, "market_share": 12.5},
            {"year": 9997, "month": 3, "make": "TestBrand", "model": "ModelB",
             "segment": "Passenger", "fuel_type": "Electric", "units_sold": 300, "market_share": 7.5},
        ]
        count = db.upsert_fcai_sales(records, pub_id)
        assert count == 2

        db.mark_publication_parsed(pub_id)

        # Verify parsed flag
        with db.cursor() as cur:
            cur.execute("SELECT parsed FROM fcai_publications WHERE id = %s", (pub_id,))
            row = cur.fetchone()
            assert row["parsed"] is True

        db.finish_run(run_id, status="completed", records_count=count)


class TestObservationStats:
    """Test stats retrieval."""

    def test_get_stats(self, db: Database):
        stats = db.get_observation_stats()
        assert "marklines_sales_count" in stats
        assert "marklines_totals_count" in stats
        assert "fcai_publications_count" in stats
        assert "fcai_sales_count" in stats


class TestCleanup:
    """Clean up test data after all tests."""

    def test_cleanup_test_data(self, db: Database):
        """Remove test data (years 9997-9999) so tests are repeatable."""
        with db.cursor() as cur:
            cur.execute("DELETE FROM fcai_sales_data WHERE year >= 9997")
            cur.execute("DELETE FROM fcai_publications WHERE year >= 9997")
            cur.execute("DELETE FROM marklines_totals WHERE year >= 9997")
            cur.execute("DELETE FROM marklines_sales WHERE year >= 9997")
            cur.execute("DELETE FROM scrape_runs WHERE source LIKE 'test%'")
