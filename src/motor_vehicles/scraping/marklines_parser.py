"""
Parse Marklines HTML pages for sales data.
Extracts HTML tables with pandas.read_html and JS chart data via regex.

Real table structure (odd-indexed tables):
  - Multi-level column headers: ('-', '-'), ('Maker/Brand', 'Maker/Brand'),
    ('2019', 'Dec.'), ('2019', 'Share'), ('2018', 'Dec.'), ...
  - Column 0: rank number
  - Column 1: Maker/Brand name
  - Remaining: year/month sales, share %, YoY, YTD values
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

# Month abbreviation to number
MONTH_ABBREV = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "may": 5, "jun": 6, "jul": 7, "aug": 8,
    "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def parse_tables(html: str, source_url: str = "") -> list[dict]:
    """Extract sales-by-make data from HTML tables.

    Marklines pages contain alternating summary/data table pairs.
    Data tables are at odd indices (1, 3, 5, ...).
    Returns list of dicts with year, month, make, units_sold.
    """
    try:
        tables = pd.read_html(StringIO(html))
    except (ValueError, ImportError):
        logger.warning("No tables found in page: %s", source_url)
        return []

    records: list[dict] = []

    for table_idx in range(0, len(tables)):
        df = tables[table_idx]
        if df.empty:
            continue

        # Detect data tables: they have a "Maker/Brand" column or multi-level headers
        # with month abbreviations. Summary tables have "Type" as first column.
        cols_flat = " ".join(str(c) for c in df.columns).lower()
        if "maker" in cols_flat or "brand" in cols_flat or "make" in cols_flat:
            new_records = _extract_from_maker_table(df, source_url)
            records.extend(new_records)

    logger.info(
        "Extracted %d sales records from tables (%s)", len(records), source_url
    )
    return records


def _extract_from_maker_table(df: pd.DataFrame, source_url: str) -> list[dict]:
    """Extract records from a Maker/Brand data table.

    Handles both multi-level and flat column headers.
    Real multi-level columns look like:
      ('-', '-'), ('Maker/Brand', 'Maker/Brand'),
      ('2019', 'Dec.'), ('2019', 'Share'), ...
    """
    records: list[dict] = []
    cols = list(df.columns)

    # Identify which columns have monthly sales data (not share/YoY)
    # Build a list of (col_index, year, month) for sales value columns
    sales_columns: list[tuple[int, int, int]] = []

    for i, col in enumerate(cols):
        year, month = _parse_column_header(col)
        if year and month:
            sales_columns.append((i, year, month))

    if not sales_columns:
        logger.debug("No sales columns identified in table")
        return records

    # Find the Maker/Brand column (usually index 1)
    make_col_idx = _find_make_column(cols)
    if make_col_idx is None:
        return records

    for _, row in df.iterrows():
        make = str(row.iloc[make_col_idx]).strip()
        if not make or make.lower() in ("total", "others", "nan", "maker/brand"):
            continue
        # Skip rows that are just numbers (rank column misidentified)
        if make.isdigit():
            continue

        for col_idx, year, month in sales_columns:
            value = row.iloc[col_idx]
            units = _parse_units(value)
            if units is not None:
                records.append({
                    "year": year,
                    "month": month,
                    "make": make,
                    "units_sold": units,
                    "source_url": source_url,
                })

    return records


def _find_make_column(cols: list) -> int | None:
    """Find the index of the Maker/Brand column."""
    for i, col in enumerate(cols):
        col_str = str(col).lower()
        if "maker" in col_str or "brand" in col_str or col_str == "make":
            return i
    return None


def _parse_column_header(col) -> tuple[int | None, int | None]:
    """Parse a column header to extract (year, month).

    Handles:
      - Multi-level tuples: ('2019', 'Dec.') -> (2019, 12)
      - Flat strings: 'Dec. 2019' -> (2019, 12)
      - Rejects: Share, Y-o-Y, Jan.-Dec. (YTD cumulative)
    Returns (year, month) or (None, None).
    """
    # Convert to string representation
    if isinstance(col, tuple):
        parts = [str(p).strip() for p in col]
    else:
        parts = [str(col).strip()]

    col_text = " ".join(parts).lower()

    # Skip share, YoY, and cumulative (Jan.-Dec., Jan.-Nov., etc.) columns
    if "share" in col_text or "y-o-y" in col_text or "yoy" in col_text:
        return None, None
    # Skip YTD cumulative columns like "Jan.-Dec.", "Jan.-Nov.", "Jan.-May"
    # These contain a range: month abbreviation + dash + another month abbreviation
    if re.search(r"jan\w*\.?\s*-\s*[a-z]", col_text):
        return None, None

    year = None
    month = None

    # Extract year (4-digit number)
    year_match = re.search(r"\b(20\d{2})\b", col_text)
    if year_match:
        year = int(year_match.group(1))

    # Extract month from abbreviation (e.g., "Dec.", "Nov.", "Jan")
    for abbrev, num in MONTH_ABBREV.items():
        # Match the abbreviation with optional period, but NOT as part of a range
        if re.search(rf"\b{abbrev}\.?\b", col_text):
            month = num
            break

    if year and month:
        return year, month
    return None, None


def _parse_units(value) -> int | None:
    """Parse a cell value into an integer unit count."""
    if pd.isna(value):
        return None
    s = str(value).strip().replace(",", "").replace(" ", "")
    if not s or s.lower() in ("nan", "-", "n/a", ""):
        return None
    # Remove percentage signs
    if "%" in s:
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
