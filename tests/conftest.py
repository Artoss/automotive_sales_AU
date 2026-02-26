"""
Shared test fixtures.
"""

from __future__ import annotations

import pytest

from motor_vehicles.config import AppConfig, load_config


@pytest.fixture
def app_config(tmp_path) -> AppConfig:
    """Create a test config with temporary directories."""
    config = AppConfig(
        database={"pg_database": "automotive_sales_au_test"},
        export={"output_dir": str(tmp_path / "exports")},
        logging={"file": str(tmp_path / "logs" / "test.log"), "console": False},
        fcai={"download_dir": str(tmp_path / "pdfs")},
    )
    return config


@pytest.fixture
def sample_marklines_html() -> str:
    """Marklines-style HTML with flash report heading, commentary,
    vehicle type table, and maker/brand table (including Others and Total)."""
    return """
    <html>
    <body>
    <h1>Automotive Sales in Australia by Month</h1>

    <h3><a id="jan"></a>Flash report, January 2024</h3>
    <p>The Australian automotive market recorded strong growth in January 2024,
    with total sales reaching 95,000 units.</p>
    <p>SUV demand continued to dominate the market.</p>

    <!-- Vehicle type table -->
    <table>
        <tr><th>Type</th><th>Jan. 2024</th><th>Jan. 2023</th><th>Y-o-Y</th></tr>
        <tr><td>Passenger Cars</td><td>30,000</td><td>28,000</td><td>7.1%</td></tr>
        <tr><td>SUV</td><td>45,000</td><td>42,000</td><td>7.1%</td></tr>
        <tr><td>LCV</td><td>15,000</td><td>14,500</td><td>3.4%</td></tr>
        <tr><td>HCV</td><td>5,000</td><td>4,800</td><td>4.2%</td></tr>
        <tr><td>Total</td><td>95,000</td><td>89,300</td><td>6.4%</td></tr>
    </table>

    <!-- Maker/Brand data table -->
    <table>
        <thead>
            <tr><th>-</th><th>Maker/Brand</th><th colspan="2">2024</th><th colspan="2">2023</th></tr>
            <tr><th>-</th><th>Maker/Brand</th><th>Jan.</th><th>Share</th><th>Jan.</th><th>Share</th></tr>
        </thead>
        <tbody>
            <tr><td>1</td><td>Toyota</td><td>15,000</td><td>15.8%</td><td>14,000</td><td>15.7%</td></tr>
            <tr><td>2</td><td>Mazda</td><td>8,000</td><td>8.4%</td><td>7,500</td><td>8.4%</td></tr>
            <tr><td>3</td><td>Hyundai</td><td>6,000</td><td>6.3%</td><td>5,800</td><td>6.5%</td></tr>
            <tr><td></td><td>Others</td><td>66,000</td><td>69.5%</td><td>62,000</td><td>69.4%</td></tr>
            <tr><td></td><td>Total</td><td>95,000</td><td>100.0%</td><td>89,300</td><td>100.0%</td></tr>
        </tbody>
    </table>

    </body>
    </html>
    """
