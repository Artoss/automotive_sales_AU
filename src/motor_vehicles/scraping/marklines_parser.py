"""
Parse Marklines HTML pages for sales data.
Extracts HTML tables with pandas.read_html and JS chart data via regex.
"""

from __future__ import annotations

import logging
import re
import unicodedata
from io import StringIO

import pandas as pd

logger = logging.getLogger("motor_vehicles.scraping.marklines_parser")

# Pattern to match embedded JavaScript chart data: Data2024 = [...]
JS_DATA_PATTERN = re.compile(r"Data(\d{4})\s*=\s*(\[[\S\s]*?\])")


def parse_tables(html: str, source_url: str = "") -> list[dict]:
    """Extract sales-by-make data from HTML tables.

    Marklines pages contain alternating header/data table pairs.
    Data tables are at odd indices (1, 3, 5, ...).
    Returns list of dicts with year, month, make, units_sold.
    """
    try:
        tables = pd.read_html(StringIO(html))
    except (ValueError, ImportError):
        logger.warning("No tables found in page: %s", source_url)
        return []

    records: list[dict] = []

    # Process data tables (odd indices)
    for table_idx in range(1, len(tables), 2):
        df = tables[table_idx]
        if df.empty:
            continue

        records.extend(_extract_records_from_table(df, source_url))

    logger.info(
        "Extracted %d sales records from tables (%s)", len(records), source_url
    )
    return records


def _extract_records_from_table(df: pd.DataFrame, source_url: str) -> list[dict]:
    """Extract year/month/make/units from a single data table."""
    records: list[dict] = []

    # The first column is typically the make (manufacturer)
    # Remaining columns are months or year-month headers
    cols = list(df.columns)
    if len(cols) < 2:
        return records

    # Month name to number mapping
    month_map = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4,
        "may": 5, "jun": 6, "jul": 7, "aug": 8,
        "sep": 9, "oct": 10, "nov": 11, "dec": 12,
        "january": 1, "february": 2, "march": 3, "april": 4,
        "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12,
    }

    for _, row in df.iterrows():
        make = str(row.iloc[0]).strip()
        if not make or make.lower() in ("total", "others", "nan"):
            continue

        for col in cols[1:]:
            col_str = str(col).strip().lower()
            # Try to parse column as month name or "month year"
            month_num = None
            year = None

            # Check direct month name
            if col_str in month_map:
                month_num = month_map[col_str]
            else:
                # Try "month year" or "year month" patterns
                parts = col_str.replace("/", " ").replace("-", " ").split()
                for part in parts:
                    if part in month_map:
                        month_num = month_map[part]
                    elif part.isdigit() and len(part) == 4:
                        year = int(part)

            if month_num is None:
                continue

            # Extract the value
            value = row[col]
            units = _parse_units(value)

            if units is not None:
                records.append({
                    "year": year or 0,
                    "month": month_num,
                    "make": make,
                    "units_sold": units,
                    "source_url": source_url,
                })

    return records


def _parse_units(value) -> int | None:
    """Parse a cell value into an integer unit count."""
    if pd.isna(value):
        return None
    s = str(value).strip().replace(",", "").replace(" ", "")
    if not s or s.lower() in ("nan", "-", "n/a", ""):
        return None
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return None


def parse_chart_data(html: str, source_url: str = "") -> list[dict]:
    """Extract total monthly sales from embedded JavaScript chart data.

    Looks for patterns like: Data2024 = [...]
    Handles both formats:
      - Array-of-arrays: [[1, 95000], [2, 88000], ...]
      - Flat key-value lines: [1, 95000, 2, 88000, ...]
    Returns list of dicts with year, month, total_units.
    """
    matches = JS_DATA_PATTERN.findall(html)
    if not matches:
        logger.debug("No chart data found in page: %s", source_url)
        return []

    records: list[dict] = []

    for year_str, raw_data in matches:
        year = int(year_str)

        # Normalize unicode (NFKC handles ideographic spaces etc.)
        normalized = unicodedata.normalize("NFKC", raw_data)

        # Try array-of-arrays format first: [1, 95000], [2, 88000]
        pair_pattern = re.findall(r"\[\s*(\d+)\s*,\s*(\d+)\s*\]", normalized)
        if pair_pattern:
            for month_str, total_str in pair_pattern:
                month = int(month_str)
                total = int(total_str)
                if 1 <= month <= 12 and total >= 100:
                    records.append({
                        "year": year,
                        "month": month,
                        "total_units": total,
                        "source_url": source_url,
                    })
            continue

        # Fallback: extract all numbers and pair them up
        lines = [line.strip() for line in normalized.split("\n")]
        numbers = []
        for line in lines:
            found = re.findall(r"\d+", line)
            if found:
                numbers.append(found[0])

        for i in range(0, len(numbers) - 1, 2):
            month = int(numbers[i])
            total = int(numbers[i + 1])
            if 1 <= month <= 12 and total >= 100:
                records.append({
                    "year": year,
                    "month": month,
                    "total_units": total,
                    "source_url": source_url,
                })

    logger.info(
        "Extracted %d total records from chart data (%s)", len(records), source_url
    )
    return records


def parse_page(html: str, source_url: str = "") -> tuple[list[dict], list[dict]]:
    """Parse a full Marklines page. Returns (sales_records, total_records)."""
    sales = parse_tables(html, source_url)
    totals = parse_chart_data(html, source_url)
    return sales, totals
