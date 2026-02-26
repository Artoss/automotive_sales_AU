"""
Tests for the FCAI publication catalog builder.
"""

from __future__ import annotations

from motor_vehicles.config import FcaiConfig
from motor_vehicles.scraping.fcai_catalog import build_catalog


class TestBuildCatalog:
    """Tests for FCAI catalog URL generation."""

    def test_builds_full_catalog(self):
        config = FcaiConfig(years=[2024], months=["january", "february"])
        catalog = build_catalog(config)
        assert len(catalog) == 2

    def test_entry_has_required_fields(self):
        config = FcaiConfig(years=[2024], months=["march"])
        catalog = build_catalog(config)
        entry = catalog[0]
        assert entry["year"] == 2024
        assert entry["month_name"] == "march"
        assert entry["month_num"] == 3
        assert "march_2024" in entry["filename"]
        assert entry["url"].startswith("https://")

    def test_filter_by_year(self):
        config = FcaiConfig(years=[2023, 2024], months=["january"])
        catalog = build_catalog(config, year=2024)
        assert len(catalog) == 1
        assert catalog[0]["year"] == 2024

    def test_filter_by_month(self):
        config = FcaiConfig(years=[2024], months=["january", "february", "march"])
        catalog = build_catalog(config, month="february")
        assert len(catalog) == 1
        assert catalog[0]["month_name"] == "february"

    def test_url_format(self):
        config = FcaiConfig(years=[2024], months=["july"])
        catalog = build_catalog(config)
        assert catalog[0]["url"] == (
            "https://www.fcai.com.au/library/publication/"
            "july_2024_vfacts_media_release_and_industry_summary.pdf"
        )

    def test_empty_months_returns_empty(self):
        config = FcaiConfig(years=[2024], months=[])
        catalog = build_catalog(config)
        assert catalog == []

    def test_multiple_years_and_months(self):
        config = FcaiConfig(
            years=[2023, 2024],
            months=["january", "february", "march"],
        )
        catalog = build_catalog(config)
        assert len(catalog) == 6  # 2 years x 3 months
