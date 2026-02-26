"""
Tests for the Marklines HTML/JS parser.
"""

from __future__ import annotations

from motor_vehicles.scraping.marklines_parser import (
    _parse_column_header,
    parse_chart_data,
    parse_tables,
)


class TestParseColumnHeader:
    """Tests for column header parsing."""

    def test_tuple_month_year(self):
        assert _parse_column_header(("2019", "Dec.")) == (2019, 12)

    def test_tuple_filters_share(self):
        assert _parse_column_header(("2019", "Share")) == (None, None)

    def test_tuple_filters_yoy(self):
        assert _parse_column_header(("Y-o-Y", "Y-o-Y")) == (None, None)

    def test_tuple_filters_ytd_range(self):
        assert _parse_column_header(("2019", "Jan.-Dec.")) == (None, None)

    def test_tuple_filters_partial_ytd(self):
        assert _parse_column_header(("2019", "Jan.-May")) == (None, None)

    def test_flat_string_month_year(self):
        assert _parse_column_header("Jan. 2019") == (2019, 1)

    def test_flat_string_filters_ytd(self):
        assert _parse_column_header("Jan.-May  2019") == (None, None)


class TestParseTables:
    """Tests for HTML table parsing."""

    def test_extracts_sales_from_data_table(self, sample_marklines_html):
        records = parse_tables(sample_marklines_html, source_url="test")
        makes = {r["make"] for r in records}
        assert "Toyota" in makes
        assert "Mazda" in makes
        assert "Hyundai" in makes
        assert "Total" not in makes

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
