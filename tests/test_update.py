"""
Tests for the monthly update module -- report models and helper functions.
"""

from __future__ import annotations

from datetime import date

from motor_vehicles.update import (
    FcaiArticlesStepReport,
    MarklinesStepReport,
    StateSalesStepReport,
    StepError,
    UpdateReport,
    _hash_pages,
    compute_marklines_years,
    run_fcai_articles_update,
    run_monthly_update,
)


class TestComputeMarklinesYears:
    """Tests for dynamic year computation."""

    def test_returns_previous_year(self):
        years = compute_marklines_years(today=date(2026, 2, 15))
        assert years == [2025]

    def test_january_returns_previous_year(self):
        years = compute_marklines_years(today=date(2026, 1, 1))
        assert years == [2025]

    def test_december_returns_previous_year(self):
        years = compute_marklines_years(today=date(2025, 12, 31))
        assert years == [2024]

    def test_returns_list(self):
        result = compute_marklines_years(today=date(2024, 6, 15))
        assert isinstance(result, list)
        assert len(result) == 1

    def test_default_uses_today(self):
        years = compute_marklines_years()
        assert isinstance(years, list)
        assert len(years) == 1
        assert years[0] == date.today().year - 1


class TestReportModels:
    """Tests for Pydantic report model construction and defaults."""

    def test_step_error_defaults(self):
        err = StepError(source="test", message="something failed")
        assert err.detail == ""

    def test_marklines_report_defaults(self):
        report = MarklinesStepReport()
        assert report.pages_fetched == 0
        assert report.sales_records == 0
        assert report.total_records == 0
        assert report.errors == []

    def test_fcai_articles_report_defaults(self):
        report = FcaiArticlesStepReport()
        assert report.articles_found == 0
        assert report.articles_already_processed == 0
        assert report.articles_new == 0

    def test_state_sales_report_defaults(self):
        report = StateSalesStepReport()
        assert report.tables_scanned == 0
        assert report.coverage_gaps == []

    def test_update_report_defaults(self):
        report = UpdateReport(timestamp="2026-01-15 10:00:00")
        assert report.marklines is None
        assert report.fcai_articles is None
        assert report.state_sales is None
        assert report.errors == []

    def test_marklines_report_skipped_unchanged_default(self):
        report = MarklinesStepReport()
        assert report.skipped_unchanged is False

    def test_update_report_serialization(self):
        report = UpdateReport(
            timestamp="2026-01-15 10:00:00",
            marklines=MarklinesStepReport(pages_fetched=2, total_records=100),
            duration_seconds=12.5,
        )
        data = report.model_dump()
        assert data["marklines"]["pages_fetched"] == 2
        assert data["duration_seconds"] == 12.5


class TestSummaryText:
    """Tests for UpdateReport.summary_text() output."""

    def test_clean_status_when_no_errors(self):
        report = UpdateReport(
            timestamp="2026-01-15 10:00:00",
            marklines=MarklinesStepReport(pages_fetched=2, total_records=100),
            fcai_articles=FcaiArticlesStepReport(articles_found=5, articles_new=1),
            state_sales=StateSalesStepReport(tables_scanned=10, months_found=8),
        )
        text = report.summary_text()
        assert "Status: CLEAN" in text
        assert "Pages fetched:    2" in text
        assert "Articles found:   5" in text
        assert "Tables scanned:   10" in text

    def test_error_count_in_status(self):
        report = UpdateReport(
            timestamp="2026-01-15 10:00:00",
            marklines=MarklinesStepReport(
                errors=[StepError(source="marklines", message="404")],
            ),
        )
        text = report.summary_text()
        assert "1 error(s)" in text
        assert "ERROR: 404" in text

    def test_coverage_gaps_displayed(self):
        report = UpdateReport(
            timestamp="2026-01-15 10:00:00",
            state_sales=StateSalesStepReport(
                tables_scanned=20,
                months_found=10,
                coverage_gaps=["2025/03", "2025/08"],
            ),
        )
        text = report.summary_text()
        assert "2025/03, 2025/08" in text

    def test_duration_displayed(self):
        report = UpdateReport(timestamp="2026-01-15 10:00:00", duration_seconds=45.3)
        text = report.summary_text()
        assert "Duration: 45.3s" in text

    def test_skipped_unchanged_displayed(self):
        report = UpdateReport(
            timestamp="2026-01-15 10:00:00",
            marklines=MarklinesStepReport(
                pages_fetched=2, skipped_unchanged=True,
            ),
        )
        text = report.summary_text()
        assert "Content unchanged" in text
        assert "Sales records" not in text

    def test_top_level_errors_displayed(self):
        report = UpdateReport(
            timestamp="2026-01-15 10:00:00",
            errors=[StepError(source="state_sales", message="DB connection failed")],
        )
        text = report.summary_text()
        assert "[state_sales] DB connection failed" in text
        assert "1 error(s)" in text


class TestHashPages:
    """Tests for _hash_pages content hashing."""

    def test_deterministic(self):
        pages = {"http://a.com": "<html>A</html>", "http://b.com": "<html>B</html>"}
        assert _hash_pages(pages) == _hash_pages(pages)

    def test_different_content_different_hash(self):
        pages1 = {"http://a.com": "<html>A</html>"}
        pages2 = {"http://a.com": "<html>B</html>"}
        assert _hash_pages(pages1) != _hash_pages(pages2)

    def test_order_independent(self):
        pages_a = {"http://a.com": "A", "http://b.com": "B"}
        pages_b = {"http://b.com": "B", "http://a.com": "A"}
        assert _hash_pages(pages_a) == _hash_pages(pages_b)

    def test_empty_pages(self):
        h = _hash_pages({})
        assert isinstance(h, str)
        assert len(h) == 64  # SHA-256 hex digest


class TestMaxPagesSignature:
    """Tests that max_pages parameter is accepted by update functions."""

    def test_run_fcai_articles_update_accepts_max_pages(self):
        """run_fcai_articles_update accepts max_pages parameter."""
        import inspect
        sig = inspect.signature(run_fcai_articles_update)
        assert "max_pages" in sig.parameters
        param = sig.parameters["max_pages"]
        assert param.default is None

    def test_run_monthly_update_accepts_max_pages(self):
        """run_monthly_update accepts max_pages parameter."""
        import inspect
        sig = inspect.signature(run_monthly_update)
        assert "max_pages" in sig.parameters
        param = sig.parameters["max_pages"]
        assert param.default is None
