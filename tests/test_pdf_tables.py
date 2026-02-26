"""
Tests for the FCAI PDF table extraction.
"""

from __future__ import annotations

from motor_vehicles.extraction.pdf_tables import (
    _identify_columns,
    _parse_filename,
)


class TestParseFilename:
    """Tests for PDF filename parsing."""

    def test_standard_filename(self):
        year, month = _parse_filename(
            "january_2024_vfacts_media_release_and_industry_summary.pdf"
        )
        assert year == 2024
        assert month == 1

    def test_july_2019(self):
        year, month = _parse_filename(
            "july_2019_vfacts_media_release_and_industry_summary.pdf"
        )
        assert year == 2019
        assert month == 7

    def test_december(self):
        year, month = _parse_filename(
            "december_2023_vfacts_media_release_and_industry_summary.pdf"
        )
        assert year == 2023
        assert month == 12

    def test_unknown_format(self):
        year, month = _parse_filename("random_file.pdf")
        assert year == 0
        assert month == 0


class TestIdentifyColumns:
    """Tests for column identification from headers."""

    def test_standard_headers(self):
        header = ["make", "model", "segment", "units sold", "market share (%)"]
        result = _identify_columns(header)
        assert result is not None
        assert result["make"] == 0
        assert result["model"] == 1
        assert result["segment"] == 2
        assert result["units_sold"] == 3
        assert result["market_share"] == 4

    def test_alternative_headers(self):
        header = ["manufacturer", "nameplate", "category", "volume", "% share"]
        result = _identify_columns(header)
        assert result is not None
        assert "make" in result
        assert "model" in result

    def test_no_recognizable_headers(self):
        header = ["foo", "bar", "baz"]
        result = _identify_columns(header)
        assert result is None
