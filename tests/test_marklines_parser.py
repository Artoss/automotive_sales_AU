"""
Tests for the Marklines HTML parser (BeautifulSoup-based).
"""

from __future__ import annotations

from motor_vehicles.scraping.marklines_parser import (
    PageResult,
    _parse_flat_column_header,
    _parse_heading_date,
    _parse_int_cell,
    _parse_pct_cell,
    parse_page,
)


class TestParseHeadingDate:
    """Tests for heading date extraction."""

    def test_full_month_name(self):
        assert _parse_heading_date("Flash report, January 2024") == (2024, 1)

    def test_abbreviated_month(self):
        assert _parse_heading_date("Flash report, Dec 2023") == (2023, 12)

    def test_no_date_returns_none(self):
        assert _parse_heading_date("Some other heading") == (None, None)


class TestParseFlatColumnHeader:
    """Tests for flat column header parsing."""

    def test_month_year(self):
        assert _parse_flat_column_header("Jan. 2024") == (2024, 1)

    def test_filters_share(self):
        assert _parse_flat_column_header("Share") == (None, None)

    def test_filters_yoy(self):
        assert _parse_flat_column_header("Y-o-Y") == (None, None)

    def test_filters_ytd_range(self):
        assert _parse_flat_column_header("Jan.-Dec. 2024") == (None, None)


class TestCellParsing:
    """Tests for individual cell value parsing."""

    def test_parse_int_with_commas(self):
        assert _parse_int_cell("15,000") == 15000

    def test_parse_int_plain(self):
        assert _parse_int_cell("8000") == 8000

    def test_parse_int_dash(self):
        assert _parse_int_cell("-") is None

    def test_parse_int_percentage(self):
        assert _parse_int_cell("15.8%") is None

    def test_parse_pct(self):
        assert _parse_pct_cell("15.8%") == 15.8

    def test_parse_pct_no_sign(self):
        assert _parse_pct_cell("7.1") == 7.1

    def test_parse_pct_dash(self):
        assert _parse_pct_cell("-") is None


class TestParsePage:
    """Tests for full page parsing."""

    def test_returns_page_result(self, sample_marklines_html):
        result = parse_page(sample_marklines_html, source_url="test")
        assert isinstance(result, PageResult)
        assert result.source_url == "test"

    def test_extracts_section(self, sample_marklines_html):
        result = parse_page(sample_marklines_html)
        assert len(result.sections) == 1
        section = result.sections[0]
        assert section.year == 2024
        assert section.month == 1

    def test_extracts_commentary(self, sample_marklines_html):
        result = parse_page(sample_marklines_html)
        commentary = result.all_commentary
        assert len(commentary) == 1
        assert "strong growth" in commentary[0]["commentary"]
        assert "SUV demand" in commentary[0]["commentary"]

    def test_extracts_maker_sales(self, sample_marklines_html):
        result = parse_page(sample_marklines_html)
        sales = result.all_maker_sales
        makes = {r["make"] for r in sales}
        assert "Toyota" in makes
        assert "Mazda" in makes
        assert "Hyundai" in makes

    def test_includes_others_row(self, sample_marklines_html):
        result = parse_page(sample_marklines_html)
        sales = result.all_maker_sales
        makes = {r["make"] for r in sales}
        assert "Others" in makes

    def test_includes_total_row(self, sample_marklines_html):
        result = parse_page(sample_marklines_html)
        sales = result.all_maker_sales
        makes = {r["make"] for r in sales}
        assert "Total" in makes

    def test_extracts_units_sold(self, sample_marklines_html):
        result = parse_page(sample_marklines_html)
        sales = result.all_maker_sales
        toyota_jan = [
            r for r in sales
            if r["make"] == "Toyota" and r["year"] == 2024 and r["month"] == 1
        ]
        assert len(toyota_jan) == 1
        assert toyota_jan[0]["units_sold"] == 15000

    def test_extracts_market_share(self, sample_marklines_html):
        result = parse_page(sample_marklines_html)
        sales = result.all_maker_sales
        toyota_2024 = [
            r for r in sales
            if r["make"] == "Toyota" and r["year"] == 2024
        ]
        assert len(toyota_2024) >= 1
        assert toyota_2024[0]["market_share"] == 15.8

    def test_extracts_vehicle_types(self, sample_marklines_html):
        result = parse_page(sample_marklines_html)
        vtypes = result.all_vehicle_type_sales
        type_names = {r["vehicle_type"] for r in vtypes}
        assert "Passenger Cars" in type_names
        assert "SUV" in type_names
        assert "LCV" in type_names
        assert "Total" in type_names

    def test_vehicle_type_units(self, sample_marklines_html):
        result = parse_page(sample_marklines_html)
        vtypes = result.all_vehicle_type_sales
        suv = [r for r in vtypes if r["vehicle_type"] == "SUV"]
        assert len(suv) == 1
        assert suv[0]["units_sold"] == 45000
        assert suv[0]["units_sold_prev_year"] == 42000

    def test_vehicle_type_yoy(self, sample_marklines_html):
        result = parse_page(sample_marklines_html)
        vtypes = result.all_vehicle_type_sales
        suv = [r for r in vtypes if r["vehicle_type"] == "SUV"]
        assert len(suv) == 1
        assert suv[0]["yoy_pct"] == 7.1

    def test_empty_html_returns_empty(self):
        result = parse_page("<html><body></body></html>")
        assert result.sections == []
        assert result.all_maker_sales == []

    def test_extracts_prev_year_data(self, sample_marklines_html):
        """Verify we get data for both 2024 and 2023 from the maker table."""
        result = parse_page(sample_marklines_html)
        sales = result.all_maker_sales
        years = {r["year"] for r in sales}
        assert 2024 in years
        assert 2023 in years
