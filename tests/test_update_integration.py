"""
Mock-based integration tests for update step orchestration.

These tests mock the DB and HTTP layers to verify that the update
functions correctly orchestrate fetching, parsing, loading, and
error handling -- without requiring a live database or network.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from motor_vehicles.update import (
    FcaiArticlesStepReport,
    MarklinesStepReport,
    StateSalesStepReport,
    UpdateReport,
    _hash_pages,
    run_fcai_articles_update,
    run_marklines_update,
    run_monthly_update,
    run_state_sales_update,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_config(app_config):
    """AppConfig with a deterministic config_hash."""
    with patch.object(type(app_config), "config_hash", return_value="testhash123"):
        yield app_config


def _make_mock_db(
    *,
    content_hash=None,
    existing_urls=None,
    tables=None,
):
    """Build a mock Database with sensible defaults."""
    db = MagicMock()
    db.connect.return_value = None
    db.close.return_value = None
    db.ensure_schema.return_value = None
    db.start_run.return_value = 1
    db.finish_run.return_value = None

    # Marklines helpers
    db.get_last_content_hash.return_value = content_hash
    db.get_latest_marklines_month.return_value = None

    # Upsert return counts
    db.upsert_marklines_sales.return_value = 5
    db.upsert_marklines_vehicle_types.return_value = 3
    db.upsert_marklines_commentary.return_value = 1

    # FCAI helpers
    db.get_existing_article_urls.return_value = existing_urls or set()
    db.upsert_fcai_article.return_value = 100
    db.upsert_fcai_article_image.return_value = 200
    db.insert_fcai_extracted_table.return_value = 300
    db.upsert_fcai_state_sales.return_value = 8

    # Cursor context manager for state sales queries
    if tables is not None:
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = tables
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        db.cursor.return_value = mock_cursor

    return db


# Patch paths target the source modules since update.py uses lazy imports
_P_ML_CLIENT = "motor_vehicles.scraping.marklines_client.MarklinesClient"
_P_ML_PARSER = "motor_vehicles.scraping.marklines_parser.parse_page"
_P_DB = "motor_vehicles.storage.database.Database"
_P_LOADER_ML = "motor_vehicles.storage.loader.load_marklines_data"
_P_LOADER_ART = "motor_vehicles.storage.loader.load_fcai_article"
_P_FCAI_SCRAPER = "motor_vehicles.scraping.fcai_articles.FcaiArticleScraper"
_P_CLASSIFY = "motor_vehicles.scraping.fcai_articles.classify_sales_article"
_P_DL_IMAGE = "motor_vehicles.extraction.image_tables.download_article_image"
_P_EXTRACT_IMG = "motor_vehicles.extraction.image_tables.extract_tables_from_image"
_P_EXTRACT_STATE = "motor_vehicles.extraction.state_sales.extract_state_sales"


# ---------------------------------------------------------------------------
# Marklines integration tests
# ---------------------------------------------------------------------------

class TestRunMarklinesUpdate:
    """Integration tests for run_marklines_update."""

    @patch(_P_LOADER_ML)
    @patch(_P_ML_PARSER)
    @patch(_P_DB)
    @patch(_P_ML_CLIENT)
    def test_successful_update(
        self, MockClient, MockDB, mock_parse, mock_load, mock_config,
    ):
        """Full successful pipeline: fetch, parse, load."""
        mock_db = _make_mock_db()
        MockDB.return_value = mock_db

        mock_client = MagicMock()
        mock_client.fetch_current_page.return_value = "<html>current</html>"
        mock_client.fetch_year_page.return_value = "<html>2025</html>"
        mock_client._build_url.return_value = "http://marklines.com/2025"
        MockClient.return_value = mock_client

        mock_result = MagicMock()
        mock_result.all_maker_sales = [{"make": "Toyota", "year": 2025, "month": 1}]
        mock_result.all_vehicle_type_sales = [{"vehicle_type": "SUV"}]
        mock_result.all_commentary = [{"commentary": "Strong"}]
        mock_parse.return_value = mock_result

        mock_load.return_value = 9

        report = run_marklines_update(mock_config)

        assert isinstance(report, MarklinesStepReport)
        assert report.pages_fetched == 2
        assert report.skipped_unchanged is False
        assert report.errors == []
        mock_db.finish_run.assert_called_once()
        # Verify content_hash was passed to finish_run
        call_kwargs = mock_db.finish_run.call_args[1]
        assert "content_hash" in call_kwargs
        assert call_kwargs["content_hash"]  # non-empty

    @patch(_P_DB)
    @patch(_P_ML_CLIENT)
    def test_skips_when_content_unchanged(self, MockClient, MockDB, mock_config):
        """Skips parse/load when content hash matches previous run."""
        pages = {
            mock_config.marklines.base_url: "<html>current</html>",
            "http://marklines.com/2025": "<html>2025</html>",
        }
        expected_hash = _hash_pages(pages)

        mock_db = _make_mock_db(content_hash=expected_hash)
        MockDB.return_value = mock_db

        mock_client = MagicMock()
        mock_client.fetch_current_page.return_value = "<html>current</html>"
        mock_client.fetch_year_page.return_value = "<html>2025</html>"
        mock_client._build_url.return_value = "http://marklines.com/2025"
        MockClient.return_value = mock_client

        report = run_marklines_update(mock_config)

        assert report.skipped_unchanged is True
        assert report.pages_fetched == 2
        assert report.total_records == 0
        mock_db.start_run.assert_not_called()

    @patch(_P_LOADER_ML)
    @patch(_P_ML_PARSER)
    @patch(_P_DB)
    @patch(_P_ML_CLIENT)
    def test_fetch_error_captured(
        self, MockClient, MockDB, mock_parse, mock_load, mock_config,
    ):
        """Errors fetching pages are captured in report, not raised."""
        mock_db = _make_mock_db()
        MockDB.return_value = mock_db

        mock_client = MagicMock()
        mock_client.fetch_current_page.side_effect = ConnectionError("timeout")
        mock_client.fetch_year_page.side_effect = ConnectionError("timeout")
        mock_client._build_url.return_value = "http://marklines.com/2025"
        MockClient.return_value = mock_client

        mock_load.return_value = 0

        report = run_marklines_update(mock_config)

        assert len(report.errors) == 2
        assert "timeout" in report.errors[0].message
        assert report.pages_fetched == 0

    @patch(_P_LOADER_ML)
    @patch(_P_ML_PARSER)
    @patch(_P_DB)
    @patch(_P_ML_CLIENT)
    def test_parse_error_captured(
        self, MockClient, MockDB, mock_parse, mock_load, mock_config,
    ):
        """Errors parsing pages are captured, not raised."""
        mock_db = _make_mock_db()
        MockDB.return_value = mock_db

        mock_client = MagicMock()
        mock_client.fetch_current_page.return_value = "<html>bad</html>"
        mock_client._build_url.return_value = "http://marklines.com/2025"
        mock_client.fetch_year_page.return_value = "<html>bad2</html>"
        MockClient.return_value = mock_client

        mock_parse.side_effect = ValueError("bad HTML")
        mock_load.return_value = 0

        report = run_marklines_update(mock_config)

        assert any("bad HTML" in e.message for e in report.errors)

    @patch(_P_LOADER_ML)
    @patch(_P_ML_PARSER)
    @patch(_P_DB)
    @patch(_P_ML_CLIENT)
    def test_load_error_marks_run_failed(
        self, MockClient, MockDB, mock_parse, mock_load, mock_config,
    ):
        """DB load failure finishes the run with 'failed' status."""
        mock_db = _make_mock_db()
        MockDB.return_value = mock_db

        mock_client = MagicMock()
        mock_client.fetch_current_page.return_value = "<html>ok</html>"
        mock_client._build_url.return_value = "http://marklines.com/2025"
        mock_client.fetch_year_page.return_value = "<html>ok2</html>"
        MockClient.return_value = mock_client

        mock_result = MagicMock()
        mock_result.all_maker_sales = []
        mock_result.all_vehicle_type_sales = []
        mock_result.all_commentary = []
        mock_parse.return_value = mock_result

        mock_load.side_effect = RuntimeError("DB write failed")

        report = run_marklines_update(mock_config)

        assert any("DB write failed" in e.message for e in report.errors)
        mock_db.finish_run.assert_called_once()
        assert mock_db.finish_run.call_args[1]["status"] == "failed"


# ---------------------------------------------------------------------------
# FCAI Articles integration tests
# ---------------------------------------------------------------------------

class TestRunFcaiArticlesUpdate:
    """Integration tests for run_fcai_articles_update."""

    @patch(_P_LOADER_ART)
    @patch(_P_EXTRACT_IMG)
    @patch(_P_DL_IMAGE)
    @patch(_P_DB)
    @patch(_P_CLASSIFY)
    @patch(_P_FCAI_SCRAPER)
    def test_skips_already_processed(
        self, MockScraper, mock_classify, MockDB,
        mock_download, mock_extract, mock_load_article, mock_config,
    ):
        """Articles already in DB are skipped."""
        from motor_vehicles.scraping.fcai_articles import ArticleListing

        mock_db = _make_mock_db(existing_urls={"http://fcai.com/old-article"})
        MockDB.return_value = mock_db

        mock_scraper = MagicMock()
        mock_scraper.fetch_article_listings.return_value = [
            ArticleListing(url="http://fcai.com/old-article", title="Jan 2025 Sales"),
        ]
        MockScraper.return_value = mock_scraper
        mock_classify.return_value = True

        report = run_fcai_articles_update(mock_config)

        assert report.articles_found == 1
        assert report.articles_already_processed == 1
        assert report.articles_new == 0
        mock_load_article.assert_not_called()

    @patch(_P_LOADER_ART)
    @patch(_P_EXTRACT_IMG)
    @patch(_P_DL_IMAGE)
    @patch(_P_DB)
    @patch(_P_CLASSIFY)
    @patch(_P_FCAI_SCRAPER)
    def test_processes_new_article_with_images(
        self, MockScraper, mock_classify, MockDB,
        mock_download, mock_extract, mock_load_article, mock_config,
    ):
        """New articles with images are downloaded, extracted, and loaded."""
        from pathlib import Path

        from motor_vehicles.scraping.fcai_articles import ArticleDetail, ArticleListing

        mock_db = _make_mock_db(existing_urls=set())
        MockDB.return_value = mock_db

        mock_scraper = MagicMock()
        mock_scraper.fetch_article_listings.return_value = [
            ArticleListing(url="http://fcai.com/new-article", title="Feb 2025 Sales"),
        ]
        mock_scraper.fetch_article.return_value = ArticleDetail(
            url="http://fcai.com/new-article",
            slug="new-article",
            title="Feb 2025 Sales",
            year=2025, month=2,
            image_urls=["http://fcai.com/img1.png", "http://fcai.com/img2.png"],
            image_labels=["Table 1", "Table 2"],
            is_sales_article=True,
        )
        MockScraper.return_value = mock_scraper
        mock_classify.return_value = True

        mock_download.return_value = Path("/tmp/img.png")
        mock_extract.return_value = [
            {"headers": ["State", "Sales"], "rows": [["NSW", "100"]]},
        ]
        mock_load_article.return_value = 2

        report = run_fcai_articles_update(mock_config)

        assert report.articles_new == 1
        assert report.images_processed == 2
        assert report.tables_extracted == 2
        mock_load_article.assert_called_once()

    @patch(_P_LOADER_ART)
    @patch(_P_DB)
    @patch(_P_CLASSIFY)
    @patch(_P_FCAI_SCRAPER)
    def test_html_table_fallback(
        self, MockScraper, mock_classify, MockDB, mock_load_article, mock_config,
    ):
        """Articles with no images but HTML tables use fallback extraction."""
        from motor_vehicles.scraping.fcai_articles import ArticleDetail, ArticleListing

        mock_db = _make_mock_db(existing_urls=set())
        MockDB.return_value = mock_db

        mock_scraper = MagicMock()
        mock_scraper.fetch_article_listings.return_value = [
            ArticleListing(url="http://fcai.com/html-article", title="Mar 2025 Sales"),
        ]
        mock_scraper.fetch_article.return_value = ArticleDetail(
            url="http://fcai.com/html-article",
            slug="html-article",
            title="Mar 2025 Sales",
            year=2025, month=3,
            image_urls=[],
            html_tables=[
                {"headers": ["State", "Sales"], "rows": [["NSW", "500"], ["VIC", "400"]]},
            ],
            is_sales_article=True,
        )
        MockScraper.return_value = mock_scraper
        mock_classify.return_value = True
        mock_load_article.return_value = 1

        report = run_fcai_articles_update(mock_config)

        assert report.articles_new == 1
        assert report.tables_extracted == 1
        assert report.images_processed == 0
        mock_load_article.assert_called_once()
        # Verify the extracted_tables dict passed to load includes html_table method
        # load_fcai_article(db, run_id, article_dict, images, extracted_tables)
        call_args = mock_load_article.call_args
        extracted = call_args[0][4]
        assert 0 in extracted
        assert extracted[0][0]["extraction_method"] == "html_table"

    @patch(_P_DB)
    @patch(_P_CLASSIFY)
    @patch(_P_FCAI_SCRAPER)
    def test_fetch_article_error_captured(
        self, MockScraper, mock_classify, MockDB, mock_config,
    ):
        """Errors fetching individual articles are captured, not raised."""
        from motor_vehicles.scraping.fcai_articles import ArticleListing

        mock_db = _make_mock_db(existing_urls=set())
        MockDB.return_value = mock_db

        mock_scraper = MagicMock()
        mock_scraper.fetch_article_listings.return_value = [
            ArticleListing(url="http://fcai.com/bad", title="Apr 2025 Sales"),
        ]
        mock_scraper.fetch_article.side_effect = ConnectionError("503")
        MockScraper.return_value = mock_scraper
        mock_classify.return_value = True

        report = run_fcai_articles_update(mock_config)

        assert report.articles_new == 1
        assert len(report.errors) == 1
        assert "503" in report.errors[0].message

    @patch(_P_CLASSIFY)
    @patch(_P_FCAI_SCRAPER)
    def test_no_sales_articles_found(self, MockScraper, mock_classify, mock_config):
        """Non-sales articles are filtered out."""
        from motor_vehicles.scraping.fcai_articles import ArticleListing

        mock_scraper = MagicMock()
        mock_scraper.fetch_article_listings.return_value = [
            ArticleListing(url="http://fcai.com/irrelevant", title="Board Meeting Minutes"),
        ]
        MockScraper.return_value = mock_scraper
        mock_classify.return_value = False

        report = run_fcai_articles_update(mock_config)

        assert report.articles_found == 0


# ---------------------------------------------------------------------------
# State Sales integration tests
# ---------------------------------------------------------------------------

class TestRunStateSalesUpdate:
    """Integration tests for run_state_sales_update."""

    @patch(_P_EXTRACT_STATE)
    @patch(_P_DB)
    def test_extracts_and_upserts(self, MockDB, mock_extract, mock_config):
        """Scans tables, extracts state data, upserts, detects coverage."""
        import json

        mock_db = _make_mock_db(tables=[
            {
                "table_id": 1,
                "headers": json.dumps(["State", "Jan 2025"]),
                "row_data": json.dumps([["NSW", "1000"], ["VIC", "800"]]),
                "year": 2025, "month": 1,
            },
            {
                "table_id": 2,
                "headers": json.dumps(["State", "Feb 2025"]),
                "row_data": json.dumps([["NSW", "1100"]]),
                "year": 2025, "month": 2,
            },
        ])
        MockDB.return_value = mock_db

        mock_extract.return_value = [
            {"year": 2025, "month": 1, "state": "NSW", "state_abbrev": "NSW",
             "units_sold": 1000, "units_sold_prev_year": None, "yoy_pct": None},
        ]

        report = run_state_sales_update(mock_config)

        assert report.tables_scanned == 2
        assert report.months_found == 2
        assert report.records_upserted == 16  # 8 per table from mock
        assert report.coverage_gaps == []

    @patch(_P_EXTRACT_STATE)
    @patch(_P_DB)
    def test_detects_coverage_gaps(self, MockDB, mock_extract, mock_config):
        """Months with no state data between first and last are reported as gaps."""
        import json

        mock_db = _make_mock_db(tables=[
            {
                "table_id": 1,
                "headers": json.dumps(["State", "Jan 2025"]),
                "row_data": json.dumps([["NSW", "1000"]]),
                "year": 2025, "month": 1,
            },
            {
                "table_id": 3,
                "headers": json.dumps(["State", "Mar 2025"]),
                "row_data": json.dumps([["NSW", "1200"]]),
                "year": 2025, "month": 3,
            },
        ])
        MockDB.return_value = mock_db

        mock_extract.return_value = [
            {"year": 2025, "month": 1, "state": "NSW", "state_abbrev": "NSW",
             "units_sold": 1000, "units_sold_prev_year": None, "yoy_pct": None},
        ]

        report = run_state_sales_update(mock_config)

        assert "2025/02" in report.coverage_gaps

    @patch(_P_EXTRACT_STATE)
    @patch(_P_DB)
    def test_no_tables(self, MockDB, mock_extract, mock_config):
        """Handles empty table set gracefully."""
        mock_db = _make_mock_db(tables=[])
        MockDB.return_value = mock_db

        report = run_state_sales_update(mock_config)

        assert report.tables_scanned == 0
        assert report.months_found == 0
        assert report.records_upserted == 0

    @patch(_P_EXTRACT_STATE)
    @patch(_P_DB)
    def test_tables_with_no_state_data(self, MockDB, mock_extract, mock_config):
        """Tables that yield no state records are skipped."""
        import json

        mock_db = _make_mock_db(tables=[
            {
                "table_id": 1,
                "headers": json.dumps(["Make", "Sales"]),
                "row_data": json.dumps([["Toyota", "5000"]]),
                "year": 2025, "month": 1,
            },
        ])
        MockDB.return_value = mock_db
        mock_extract.return_value = []

        report = run_state_sales_update(mock_config)

        assert report.tables_scanned == 1
        assert report.months_found == 0
        assert report.records_upserted == 0


# ---------------------------------------------------------------------------
# Full orchestrator tests
# ---------------------------------------------------------------------------

class TestRunMonthlyUpdate:
    """Integration tests for the top-level run_monthly_update orchestrator."""

    @patch("motor_vehicles.update.run_marklines_update")
    @patch("motor_vehicles.update.run_fcai_articles_update")
    @patch("motor_vehicles.update.run_state_sales_update")
    @patch("motor_vehicles.update._run_quality_checks")
    def test_orchestrates_all_steps(
        self, mock_quality, mock_state, mock_fcai, mock_marklines, mock_config,
    ):
        """All four steps are called and results aggregated."""
        mock_marklines.return_value = MarklinesStepReport(
            pages_fetched=2, total_records=50,
        )
        mock_fcai.return_value = FcaiArticlesStepReport(
            articles_found=3, articles_new=1,
        )
        mock_state.return_value = StateSalesStepReport(
            tables_scanned=5, months_found=4,
        )
        mock_quality.return_value = []

        report = run_monthly_update(mock_config)

        assert isinstance(report, UpdateReport)
        assert report.marklines.pages_fetched == 2
        assert report.fcai_articles.articles_new == 1
        assert report.state_sales.months_found == 4
        assert report.errors == []
        assert report.duration_seconds >= 0

    @patch("motor_vehicles.update.run_marklines_update")
    @patch("motor_vehicles.update.run_fcai_articles_update")
    @patch("motor_vehicles.update.run_state_sales_update")
    @patch("motor_vehicles.update._run_quality_checks")
    def test_marklines_failure_continues_pipeline(
        self, mock_quality, mock_state, mock_fcai, mock_marklines, mock_config,
    ):
        """If Marklines step raises, FCAI and state sales still run."""
        mock_marklines.side_effect = RuntimeError("Marklines down")
        mock_fcai.return_value = FcaiArticlesStepReport(articles_found=2)
        mock_state.return_value = StateSalesStepReport(tables_scanned=3)
        mock_quality.return_value = []

        report = run_monthly_update(mock_config)

        assert report.marklines is None
        assert len(report.errors) == 1
        assert "Marklines down" in report.errors[0].message
        assert report.fcai_articles is not None
        assert report.state_sales is not None

    @patch("motor_vehicles.update.run_marklines_update")
    @patch("motor_vehicles.update.run_fcai_articles_update")
    @patch("motor_vehicles.update.run_state_sales_update")
    @patch("motor_vehicles.update._run_quality_checks")
    def test_quality_failure_non_fatal(
        self, mock_quality, mock_state, mock_fcai, mock_marklines, mock_config,
    ):
        """Quality check failure is non-fatal."""
        mock_marklines.return_value = MarklinesStepReport()
        mock_fcai.return_value = FcaiArticlesStepReport()
        mock_state.return_value = StateSalesStepReport()
        mock_quality.side_effect = RuntimeError("quality DB error")

        report = run_monthly_update(mock_config)

        assert report.errors == []
        assert report.quality_issues == []

    @patch("motor_vehicles.update.run_marklines_update")
    @patch("motor_vehicles.update.run_fcai_articles_update")
    @patch("motor_vehicles.update.run_state_sales_update")
    @patch("motor_vehicles.update._run_quality_checks")
    def test_quality_issues_included(
        self, mock_quality, mock_state, mock_fcai, mock_marklines, mock_config,
    ):
        """Quality issues are included in the report."""
        mock_marklines.return_value = MarklinesStepReport()
        mock_fcai.return_value = FcaiArticlesStepReport()
        mock_state.return_value = StateSalesStepReport()
        mock_quality.return_value = [
            {"check": "totals", "severity": "warning", "message": "3% diff", "detail": ""},
        ]

        report = run_monthly_update(mock_config)

        assert len(report.quality_issues) == 1
        assert report.quality_issues[0]["check"] == "totals"

    @patch("motor_vehicles.update.run_marklines_update")
    @patch("motor_vehicles.update.run_fcai_articles_update")
    @patch("motor_vehicles.update.run_state_sales_update")
    @patch("motor_vehicles.update._run_quality_checks")
    def test_summary_text_generated(
        self, mock_quality, mock_state, mock_fcai, mock_marklines, mock_config,
    ):
        """The report generates valid summary text."""
        mock_marklines.return_value = MarklinesStepReport(
            pages_fetched=2, total_records=100,
        )
        mock_fcai.return_value = FcaiArticlesStepReport(
            articles_found=5, articles_new=2, tables_extracted=4,
        )
        mock_state.return_value = StateSalesStepReport(
            tables_scanned=10, months_found=8,
        )
        mock_quality.return_value = []

        report = run_monthly_update(mock_config)
        text = report.summary_text()

        assert "Monthly Update Report" in text
        assert "Marklines" in text
        assert "FCAI Articles" in text
        assert "State/Territory Sales" in text
        assert "Status: CLEAN" in text

    @patch("motor_vehicles.update.run_marklines_update")
    @patch("motor_vehicles.update.run_fcai_articles_update")
    @patch("motor_vehicles.update.run_state_sales_update")
    @patch("motor_vehicles.update._run_quality_checks")
    def test_skipped_marklines_in_summary(
        self, mock_quality, mock_state, mock_fcai, mock_marklines, mock_config,
    ):
        """Skipped Marklines shows in summary text."""
        mock_marklines.return_value = MarklinesStepReport(
            pages_fetched=2, skipped_unchanged=True,
        )
        mock_fcai.return_value = FcaiArticlesStepReport()
        mock_state.return_value = StateSalesStepReport()
        mock_quality.return_value = []

        report = run_monthly_update(mock_config)
        text = report.summary_text()

        assert "Content unchanged" in text

    @patch("motor_vehicles.update.run_marklines_update")
    @patch("motor_vehicles.update.run_fcai_articles_update")
    @patch("motor_vehicles.update.run_state_sales_update")
    @patch("motor_vehicles.update._run_quality_checks")
    def test_max_pages_passed_to_fcai(
        self, mock_quality, mock_state, mock_fcai, mock_marklines, mock_config,
    ):
        """max_pages parameter is forwarded to run_fcai_articles_update."""
        mock_marklines.return_value = MarklinesStepReport()
        mock_fcai.return_value = FcaiArticlesStepReport()
        mock_state.return_value = StateSalesStepReport()
        mock_quality.return_value = []

        run_monthly_update(mock_config, max_pages=15)

        mock_fcai.assert_called_once_with(mock_config, max_pages=15)

    @patch("motor_vehicles.update.run_marklines_update")
    @patch("motor_vehicles.update.run_fcai_articles_update")
    @patch("motor_vehicles.update.run_state_sales_update")
    @patch("motor_vehicles.update._run_quality_checks")
    def test_max_pages_default_none(
        self, mock_quality, mock_state, mock_fcai, mock_marklines, mock_config,
    ):
        """Default max_pages is None (uses config value)."""
        mock_marklines.return_value = MarklinesStepReport()
        mock_fcai.return_value = FcaiArticlesStepReport()
        mock_state.return_value = StateSalesStepReport()
        mock_quality.return_value = []

        run_monthly_update(mock_config)

        mock_fcai.assert_called_once_with(mock_config, max_pages=None)


# ---------------------------------------------------------------------------
# FCAI Articles max_pages passthrough tests
# ---------------------------------------------------------------------------

class TestFcaiArticlesMaxPages:
    """Tests for max_pages parameter passthrough in run_fcai_articles_update."""

    @patch(_P_CLASSIFY)
    @patch(_P_FCAI_SCRAPER)
    def test_max_pages_passed_to_fetch_listings(
        self, MockScraper, mock_classify, mock_config,
    ):
        """max_pages is forwarded to scraper.fetch_article_listings()."""
        from motor_vehicles.scraping.fcai_articles import ArticleListing

        mock_scraper = MagicMock()
        mock_scraper.fetch_article_listings.return_value = []
        MockScraper.return_value = mock_scraper
        mock_classify.return_value = False

        run_fcai_articles_update(mock_config, max_pages=10)

        mock_scraper.fetch_article_listings.assert_called_once_with(max_pages=10)

    @patch(_P_CLASSIFY)
    @patch(_P_FCAI_SCRAPER)
    def test_max_pages_none_uses_default(
        self, MockScraper, mock_classify, mock_config,
    ):
        """When max_pages is None, fetch_article_listings uses its default."""
        mock_scraper = MagicMock()
        mock_scraper.fetch_article_listings.return_value = []
        MockScraper.return_value = mock_scraper
        mock_classify.return_value = False

        run_fcai_articles_update(mock_config)

        mock_scraper.fetch_article_listings.assert_called_once_with(max_pages=None)


# ---------------------------------------------------------------------------
# Multi-category listing tests
# ---------------------------------------------------------------------------

class TestFetchAllCategoryListings:
    """Tests for FcaiArticleScraper.fetch_all_category_listings."""

    def test_deduplicates_across_categories(self, mock_config):
        """Articles appearing in multiple categories are deduplicated."""
        from motor_vehicles.scraping.fcai_articles import ArticleListing, FcaiArticleScraper

        scraper = FcaiArticleScraper(mock_config.http, mock_config.fcai.articles)

        # Mock fetch_article_listings to return overlapping results
        call_count = 0
        def mock_fetch(max_pages=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return [
                    ArticleListing(url="http://fcai.com/a", title="Article A"),
                    ArticleListing(url="http://fcai.com/b", title="Article B"),
                ]
            else:
                return [
                    ArticleListing(url="http://fcai.com/b", title="Article B"),  # duplicate
                    ArticleListing(url="http://fcai.com/c", title="Article C"),
                ]

        scraper.fetch_article_listings = mock_fetch

        results = scraper.fetch_all_category_listings(
            categories=["media-release", "news"], max_pages=5,
        )

        assert len(results) == 3
        urls = [r.url for r in results]
        assert urls == ["http://fcai.com/a", "http://fcai.com/b", "http://fcai.com/c"]

    def test_uses_config_categories_by_default(self, mock_config):
        """Uses backfill_categories from config when categories not specified."""
        from motor_vehicles.scraping.fcai_articles import FcaiArticleScraper

        scraper = FcaiArticleScraper(mock_config.http, mock_config.fcai.articles)

        categories_seen = []
        original_fetch = scraper.fetch_article_listings
        def mock_fetch(max_pages=None):
            # Record which category params are set at call time
            categories_seen.append(
                scraper.articles_config.listing_params.get("_sft_category")
            )
            return []

        scraper.fetch_article_listings = mock_fetch
        scraper.fetch_all_category_listings()

        assert categories_seen == ["media-release", "news"]

    def test_restores_listing_params_after_fetch(self, mock_config):
        """listing_params are restored even if fetch raises."""
        from motor_vehicles.scraping.fcai_articles import FcaiArticleScraper

        scraper = FcaiArticleScraper(mock_config.http, mock_config.fcai.articles)
        original_params = dict(scraper.articles_config.listing_params)

        def mock_fetch(max_pages=None):
            raise ConnectionError("test")

        scraper.fetch_article_listings = mock_fetch

        try:
            scraper.fetch_all_category_listings(categories=["news"])
        except ConnectionError:
            pass

        assert scraper.articles_config.listing_params == original_params
