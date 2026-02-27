"""
Tests for data quality check report models.
"""

from __future__ import annotations

from motor_vehicles.quality import QualityIssue, QualityReport


class TestQualityIssue:
    """Tests for individual quality issue model."""

    def test_defaults(self):
        issue = QualityIssue(check="test", severity="warning", message="something")
        assert issue.detail == ""

    def test_all_fields(self):
        issue = QualityIssue(
            check="marklines_totals",
            severity="error",
            message="mismatch",
            detail="2025/01",
        )
        assert issue.check == "marklines_totals"
        assert issue.severity == "error"


class TestQualityReport:
    """Tests for aggregated quality report."""

    def test_empty_report(self):
        report = QualityReport()
        assert not report.has_errors
        assert not report.has_warnings
        assert "all passed" in report.summary_text()

    def test_has_errors(self):
        report = QualityReport(issues=[
            QualityIssue(check="test", severity="error", message="bad"),
        ])
        assert report.has_errors
        assert report.has_warnings is False

    def test_has_warnings(self):
        report = QualityReport(issues=[
            QualityIssue(check="test", severity="warning", message="eh"),
        ])
        assert report.has_warnings
        assert not report.has_errors

    def test_summary_text_with_issues(self):
        report = QualityReport(issues=[
            QualityIssue(check="low_count", severity="warning", message="only 5 makes"),
            QualityIssue(check="total_mismatch", severity="error", message="off by 100"),
        ])
        text = report.summary_text()
        assert "[WARN]" in text
        assert "[ERROR]" in text
        assert "low_count" in text
        assert "total_mismatch" in text

    def test_summary_text_with_detail(self):
        report = QualityReport(issues=[
            QualityIssue(
                check="duplicates",
                severity="warning",
                message="2 articles",
                detail="slug-a, slug-b",
            ),
        ])
        text = report.summary_text()
        assert "slug-a, slug-b" in text
