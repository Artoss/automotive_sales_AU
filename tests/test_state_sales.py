"""
Tests for State/Territory sales extraction from article tables.
"""

from __future__ import annotations

from motor_vehicles.extraction.state_sales import (
    _find_state_column,
    extract_state_sales,
    parse_float,
    parse_int,
)


class TestParseInt:
    """Tests for integer parsing helper."""

    def test_plain_number(self):
        assert parse_int("1234") == 1234

    def test_comma_separated(self):
        assert parse_int("1,234") == 1234

    def test_with_spaces(self):
        assert parse_int(" 1234 ") == 1234

    def test_empty_string(self):
        assert parse_int("") is None

    def test_none_value(self):
        assert parse_int(None) is None

    def test_non_numeric(self):
        assert parse_int("abc") is None

    def test_large_number(self):
        assert parse_int("1,234,567") == 1234567


class TestParseFloat:
    """Tests for float parsing helper."""

    def test_plain_float(self):
        assert parse_float("3.5") == 3.5

    def test_negative(self):
        assert parse_float("-3.5") == -3.5

    def test_with_percent(self):
        assert parse_float("2.0%") == 2.0

    def test_empty_string(self):
        assert parse_float("") is None

    def test_non_numeric(self):
        assert parse_float("N/A") is None


class TestFindStateColumn:
    """Tests for column detection logic."""

    def test_finds_by_header_name(self):
        headers = ["Rank", "Vehicle", "State/Territory", "Sales"]
        result = _find_state_column(headers, [])
        assert result == 2

    def test_finds_territory_in_header(self):
        headers = ["Territory", "Jan 2025", "Jan 2024", "% Change"]
        result = _find_state_column(headers, [])
        assert result == 0

    def test_finds_by_row_data_col_0(self):
        headers = ["Col1", "Col2", "Col3", "Col4"]
        rows = [
            ["New South Wales", "1000", "900", "11.1"],
            ["Victoria", "800", "750", "6.7"],
            ["Queensland", "600", "580", "3.4"],
            ["South Australia", "200", "190", "5.3"],
        ]
        result = _find_state_column(headers, rows)
        assert result == 0

    def test_returns_none_when_no_states(self):
        headers = ["Make", "Model", "Units"]
        rows = [
            ["Toyota", "Corolla", "1000"],
            ["Mazda", "CX-5", "800"],
        ]
        result = _find_state_column(headers, rows)
        assert result is None


class TestExtractStateSales:
    """Tests for full extraction pipeline."""

    def test_clean_4_column_table(self):
        headers = ["State/Territory", "Jan 2025", "Jan 2024", "% Change"]
        rows = [
            ["New South Wales", "12,345", "11,000", "12.2"],
            ["Victoria", "10,000", "9,500", "5.3"],
            ["Queensland", "8,000", "7,800", "2.6"],
            ["South Australia", "3,000", "2,900", "3.4"],
            ["Western Australia", "5,000", "4,800", "4.2"],
            ["Tasmania", "1,200", "1,100", "9.1"],
            ["Northern Territory", "500", "480", "4.2"],
            ["Australian Capital Territory", "1,500", "1,400", "7.1"],
            ["Total", "41,545", "38,980", "6.6"],
        ]
        records = extract_state_sales(headers, rows, 2025, 1)
        assert len(records) == 9  # 8 states + total
        nsw = next(r for r in records if r["state_abbrev"] == "NSW")
        assert nsw["units_sold"] == 12345
        assert nsw["units_sold_prev_year"] == 11000
        assert nsw["yoy_pct"] == 12.2
        assert nsw["year"] == 2025
        assert nsw["month"] == 1

    def test_total_row_included(self):
        headers = ["State/Territory", "Sales", "Prev", "Change"]
        rows = [
            ["Victoria", "1,000", "900", "11.1"],
            ["Total", "5,000", "4,500", "11.1"],
        ]
        records = extract_state_sales(headers, rows, 2025, 6)
        total = next(r for r in records if r["state_abbrev"] == "TOTAL")
        assert total["units_sold"] == 5000

    def test_empty_table(self):
        assert extract_state_sales([], [], 2025, 1) == []

    def test_non_state_rows_skipped(self):
        headers = ["State/Territory", "Sales", "Prev", "Change"]
        rows = [
            ["Victoria", "1,000", "900", "11.1"],
            ["Some Other Row", "500", "400", "25.0"],
            ["Queensland", "800", "750", "6.7"],
        ]
        records = extract_state_sales(headers, rows, 2025, 3)
        assert len(records) == 2
        states = {r["state_abbrev"] for r in records}
        assert states == {"VIC", "QLD"}

    def test_json_string_inputs(self):
        """Headers/row_data can arrive as JSON strings from DB."""
        import json
        headers = json.dumps(["State/Territory", "Sales", "Prev", "Change"])
        rows = json.dumps([
            ["New South Wales", "1,000", "900", "11.1"],
        ])
        records = extract_state_sales(headers, rows, 2025, 5)
        assert len(records) == 1
        assert records[0]["state_abbrev"] == "NSW"

    def test_missing_units_skipped(self):
        headers = ["State/Territory", "Sales", "Prev", "Change"]
        rows = [
            ["Victoria", "", "900", ""],
            ["Queensland", "800", "750", "6.7"],
        ]
        records = extract_state_sales(headers, rows, 2025, 1)
        assert len(records) == 1
        assert records[0]["state_abbrev"] == "QLD"

    def test_state_name_title_case_in_output(self):
        headers = ["State/Territory", "Sales", "Prev", "Change"]
        rows = [
            ["new south wales", "1,000", "900", "11.1"],
        ]
        records = extract_state_sales(headers, rows, 2025, 1)
        assert records[0]["state"] == "New South Wales"
