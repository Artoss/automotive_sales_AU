"""
Tests for the Marklines HTML/JS parser.
"""

from __future__ import annotations

from motor_vehicles.scraping.marklines_parser import (
    parse_chart_data,
    parse_tables,
)


class TestParseTables:
    """Tests for HTML table parsing."""

    def test_extracts_sales_from_data_table(self, sample_marklines_html):
        records = parse_tables(sample_marklines_html, source_url="test")
        # Should extract Toyota, Mazda, Hyundai (skip Total) x 3 months
        makes = {r["make"] for r in records}
        assert "Toyota" in makes
        assert "Mazda" in makes
        assert "Hyundai" in makes
        assert "Total" not in makes

    def test_extracts_month_numbers(self, sample_marklines_html):
        records = parse_tables(sample_marklines_html, source_url="test")
        months = {r["month"] for r in records}
        assert 1 in months  # Jan
        assert 2 in months  # Feb
        assert 3 in months  # Mar

    def test_extracts_units_sold(self, sample_marklines_html):
        records = parse_tables(sample_marklines_html, source_url="test")
        toyota_jan = [r for r in records if r["make"] == "Toyota" and r["month"] == 1]
        assert len(toyota_jan) >= 1
        assert toyota_jan[0]["units_sold"] == 15000

    def test_empty_html_returns_empty(self):
        records = parse_tables("<html><body></body></html>")
        assert records == []


class TestParseChartData:
    """Tests for JavaScript chart data extraction."""

    def test_extracts_yearly_totals(self, sample_marklines_html):
        records = parse_chart_data(sample_marklines_html, source_url="test")
        assert len(records) > 0
        years = {r["year"] for r in records}
        assert 2024 in years

    def test_extracts_monthly_values(self, sample_marklines_html):
        records = parse_chart_data(sample_marklines_html, source_url="test")
        jan = [r for r in records if r["year"] == 2024 and r["month"] == 1]
        assert len(jan) == 1
        assert jan[0]["total_units"] == 95000

    def test_no_chart_data_returns_empty(self):
        records = parse_chart_data("<html><body></body></html>")
        assert records == []
