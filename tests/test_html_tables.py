"""
Tests for HTML table extraction from FCAI articles.
"""

from __future__ import annotations

from motor_vehicles.scraping.fcai_articles import _parse_html_table

from bs4 import BeautifulSoup


def _make_table(html: str):
    """Parse HTML string and return the first <table> element."""
    soup = BeautifulSoup(html, "lxml")
    return soup.find("table")


class TestParseHtmlTable:
    """Tests for _parse_html_table helper."""

    def test_simple_table_with_thead(self):
        html = """
        <table>
            <thead><tr><th>State</th><th>Sales</th></tr></thead>
            <tbody>
                <tr><td>NSW</td><td>1,234</td></tr>
                <tr><td>VIC</td><td>5,678</td></tr>
            </tbody>
        </table>
        """
        result = _parse_html_table(_make_table(html))
        assert result["headers"] == ["State", "Sales"]
        assert len(result["rows"]) == 2
        assert result["rows"][0] == ["NSW", "1,234"]
        assert result["rows"][1] == ["VIC", "5,678"]

    def test_table_without_thead(self):
        html = """
        <table>
            <tr><th>Make</th><th>Units</th></tr>
            <tr><td>Toyota</td><td>15000</td></tr>
        </table>
        """
        result = _parse_html_table(_make_table(html))
        assert result["headers"] == ["Make", "Units"]
        assert len(result["rows"]) == 1
        assert result["rows"][0] == ["Toyota", "15000"]

    def test_table_no_headers(self):
        html = """
        <table>
            <tr><td>Row1</td><td>100</td></tr>
            <tr><td>Row2</td><td>200</td></tr>
        </table>
        """
        result = _parse_html_table(_make_table(html))
        assert result["headers"] == []
        assert len(result["rows"]) == 2

    def test_empty_table(self):
        html = "<table></table>"
        result = _parse_html_table(_make_table(html))
        assert result["headers"] == []
        assert result["rows"] == []

    def test_whitespace_stripped(self):
        html = """
        <table>
            <tr><th> State </th><th> Sales </th></tr>
            <tr><td>  NSW  </td><td> 1,000 </td></tr>
        </table>
        """
        result = _parse_html_table(_make_table(html))
        assert result["headers"] == ["State", "Sales"]
        assert result["rows"][0] == ["NSW", "1,000"]

    def test_state_territory_table(self):
        """Realistic FCAI state/territory table."""
        html = """
        <table>
            <thead>
                <tr>
                    <th>State/Territory</th>
                    <th>Jan 2025</th>
                    <th>Jan 2024</th>
                    <th>% Change</th>
                </tr>
            </thead>
            <tbody>
                <tr><td>New South Wales</td><td>12,345</td><td>11,000</td><td>12.2%</td></tr>
                <tr><td>Victoria</td><td>10,000</td><td>9,500</td><td>5.3%</td></tr>
                <tr><td>Queensland</td><td>8,000</td><td>7,800</td><td>2.6%</td></tr>
                <tr><td>Total</td><td>30,345</td><td>28,300</td><td>7.2%</td></tr>
            </tbody>
        </table>
        """
        result = _parse_html_table(_make_table(html))
        assert result["headers"] == ["State/Territory", "Jan 2025", "Jan 2024", "% Change"]
        assert len(result["rows"]) == 4
        assert result["rows"][0][0] == "New South Wales"
        assert result["rows"][3][0] == "Total"
