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
    """Test marklines_sales upserts with new columns."""

    def test_upsert_sales(self, db: Database):
        run_id = db.start_run(source="test_marklines")

        records = [
            {
                "year": 9999, "month": 1, "make": "TestMake",
                "units_sold": 100, "market_share": 12.5,
                "units_sold_prev_year": 90, "yoy_pct": 11.1,
                "source_url": "http://test",
            },
            {
                "year": 9999, "month": 2, "make": "TestMake",
                "units_sold": 200, "market_share": None,
                "units_sold_prev_year": None, "yoy_pct": None,
                "source_url": "http://test",
            },
        ]
        count = db.upsert_marklines_sales(records, run_id)
        assert count == 2

        # Upsert again with updated values
        records[0]["units_sold"] = 150
        records[0]["market_share"] = 14.2
        count = db.upsert_marklines_sales(records, run_id)
        assert count == 2

        # Verify the update took effect
        with db.cursor() as cur:
            cur.execute(
                "SELECT units_sold, market_share, units_sold_prev_year, yoy_pct "
                "FROM marklines_sales WHERE year = 9999 AND month = 1 AND make = 'TestMake'"
            )
            row = cur.fetchone()
            assert row["units_sold"] == 150
            assert float(row["market_share"]) == 14.2
            assert row["units_sold_prev_year"] == 90
            assert float(row["yoy_pct"]) == 11.1

        db.finish_run(run_id, status="completed", records_count=count)

    def test_upsert_others_row(self, db: Database):
        """Verify 'Others' is now stored (was previously filtered)."""
        run_id = db.start_run(source="test_others")

        records = [
            {
                "year": 9999, "month": 1, "make": "Others",
                "units_sold": 50000, "market_share": 65.0,
                "units_sold_prev_year": 48000, "yoy_pct": 4.2,
                "source_url": "http://test",
            },
        ]
        count = db.upsert_marklines_sales(records, run_id)
        assert count == 1

        with db.cursor() as cur:
            cur.execute(
                "SELECT make, units_sold FROM marklines_sales "
                "WHERE year = 9999 AND month = 1 AND make = 'Others'"
            )
            row = cur.fetchone()
            assert row is not None
            assert row["make"] == "Others"
            assert row["units_sold"] == 50000

        db.finish_run(run_id, status="completed", records_count=count)


class TestMarklinesVehicleTypes:
    """Test marklines_vehicle_type_sales upserts."""

    def test_upsert_vehicle_types(self, db: Database):
        run_id = db.start_run(source="test_vtypes")

        records = [
            {
                "year": 9999, "month": 1, "vehicle_type": "Passenger Cars",
                "units_sold": 30000, "units_sold_prev_year": 28000, "yoy_pct": 7.1,
                "source_url": "http://test",
            },
            {
                "year": 9999, "month": 1, "vehicle_type": "SUV",
                "units_sold": 45000, "units_sold_prev_year": 42000, "yoy_pct": 7.1,
                "source_url": "http://test",
            },
        ]
        count = db.upsert_marklines_vehicle_types(records, run_id)
        assert count == 2

        # Verify data
        with db.cursor() as cur:
            cur.execute(
                "SELECT vehicle_type, units_sold FROM marklines_vehicle_type_sales "
                "WHERE year = 9999 AND month = 1 ORDER BY vehicle_type"
            )
            rows = cur.fetchall()
            assert len(rows) == 2
            types = {r["vehicle_type"] for r in rows}
            assert "Passenger Cars" in types
            assert "SUV" in types

        db.finish_run(run_id, status="completed", records_count=count)


class TestMarklinesCommentary:
    """Test marklines_commentary upserts."""

    def test_upsert_commentary(self, db: Database):
        run_id = db.start_run(source="test_commentary")

        records = [
            {
                "year": 9999, "month": 1,
                "report_date": "Flash report, January 9999",
                "commentary": "The market showed strong growth.",
                "source_url": "http://test",
            },
        ]
        count = db.upsert_marklines_commentary(records, run_id)
        assert count == 1

        # Verify data
        with db.cursor() as cur:
            cur.execute(
                "SELECT commentary, report_date FROM marklines_commentary "
                "WHERE year = 9999 AND month = 1"
            )
            row = cur.fetchone()
            assert row is not None
            assert "strong growth" in row["commentary"]

        # Upsert with updated text
        records[0]["commentary"] = "Updated commentary text."
        count = db.upsert_marklines_commentary(records, run_id)
        assert count == 1

        with db.cursor() as cur:
            cur.execute(
                "SELECT commentary FROM marklines_commentary "
                "WHERE year = 9999 AND month = 1"
            )
            row = cur.fetchone()
            assert row["commentary"] == "Updated commentary text."

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
        assert "marklines_vtype_count" in stats
        assert "marklines_commentary_count" in stats
        assert "fcai_publications_count" in stats
        assert "fcai_sales_count" in stats


class TestCleanup:
    """Clean up test data after all tests."""

    def test_cleanup_test_data(self, db: Database):
        """Remove test data (years 9997-9999) so tests are repeatable."""
        with db.cursor() as cur:
            cur.execute("DELETE FROM fcai_sales_data WHERE year >= 9997")
            cur.execute("DELETE FROM fcai_publications WHERE year >= 9997")
            cur.execute("DELETE FROM marklines_commentary WHERE year >= 9997")
            cur.execute("DELETE FROM marklines_vehicle_type_sales WHERE year >= 9997")
            cur.execute("DELETE FROM marklines_sales WHERE year >= 9997")
            cur.execute("DELETE FROM scrape_runs WHERE source LIKE 'test%'")
