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
    """Minimal Marklines-style HTML with tables and chart data."""
    return """
    <html>
    <body>
    <h1>Automotive Sales in Australia by Month</h1>

    <!-- Header table (index 0) -->
    <table>
        <tr><th>Summary</th><th>2024</th></tr>
        <tr><td>Total</td><td>1000000</td></tr>
    </table>

    <!-- Data table (index 1) -->
    <table>
        <tr><th>Make</th><th>Jan</th><th>Feb</th><th>Mar</th></tr>
        <tr><td>Toyota</td><td>15000</td><td>14500</td><td>16000</td></tr>
        <tr><td>Mazda</td><td>8000</td><td>7500</td><td>8200</td></tr>
        <tr><td>Hyundai</td><td>6000</td><td>5800</td><td>6300</td></tr>
        <tr><td>Total</td><td>29000</td><td>27800</td><td>30500</td></tr>
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
