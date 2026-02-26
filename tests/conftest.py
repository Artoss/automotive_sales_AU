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
    """Minimal Marklines-style HTML mimicking real Marklines table structure."""
    return """
    <html>
    <body>
    <h1>Automotive Sales in Australia by Month</h1>

    <!-- Summary table (even index) -->
    <table>
        <tr><th>Type</th><th>Jan. 2024</th><th>Jan. 2023</th></tr>
        <tr><td>Passenger Cars</td><td>50000</td><td>48000</td></tr>
    </table>

    <!-- Data table with Maker/Brand (odd index) -->
    <table>
        <thead>
            <tr><th>-</th><th>Maker/Brand</th><th colspan="2">2024</th><th colspan="2">2023</th></tr>
            <tr><th>-</th><th>Maker/Brand</th><th>Jan.</th><th>Share</th><th>Jan.</th><th>Share</th></tr>
        </thead>
        <tbody>
            <tr><td>1</td><td>Toyota</td><td>15000</td><td>20.5%</td><td>14000</td><td>19.8%</td></tr>
            <tr><td>2</td><td>Mazda</td><td>8000</td><td>10.9%</td><td>7500</td><td>10.6%</td></tr>
            <tr><td>3</td><td>Hyundai</td><td>6000</td><td>8.2%</td><td>5800</td><td>8.2%</td></tr>
            <tr><td></td><td>Total</td><td>29000</td><td></td><td>27300</td><td></td></tr>
        </tbody>
    </table>

    <script>
    Data2024 = [
        [1, 95000],
        [2, 88000],
        [3, 102000],
        [4, 91000],
    ]
    </script>
    </body>
    </html>
    """
