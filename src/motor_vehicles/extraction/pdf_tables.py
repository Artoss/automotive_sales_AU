"""
Extract tabular data from FCAI PDF publications using pdfplumber.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import pdfplumber

logger = logging.getLogger("motor_vehicles.extraction.pdf_tables")

# Month names for parsing from filename
MONTH_NAME_TO_NUM = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def extract_tables_from_pdf(filepath: str | Path) -> list[dict]:
    """Extract sales data tables from an FCAI PDF.

    Returns list of dicts with: year, month, make, model, segment,
    fuel_type, units_sold, market_share.
    """
    filepath = Path(filepath)
    if not filepath.exists():
        logger.warning("PDF not found: %s", filepath)
        return []

    # Parse year and month from filename
    year, month = _parse_filename(filepath.name)

    records: list[dict] = []

    try:
        with pdfplumber.open(filepath) as pdf:
            for page_num, page in enumerate(pdf.pages):
                tables = page.extract_tables()
                if not tables:
                    continue

                for table in tables:
                    page_records = _parse_table(table, year, month)
                    records.extend(page_records)

                logger.debug(
                    "Page %d of %s: %d tables found",
                    page_num + 1, filepath.name, len(tables),
                )

    except Exception as e:
        logger.error("Failed to extract tables from %s: %s", filepath.name, e)
        return []

    logger.info("Extracted %d records from %s", len(records), filepath.name)
    return records


def _parse_filename(filename: str) -> tuple[int, int]:
    """Parse year and month from FCAI filename.

    Expected format: january_2024_vfacts_media_release_and_industry_summary.pdf
    """
    name = filename.lower()
    year = 0
    month = 0

    # Extract year
    year_match = re.search(r"(\d{4})", name)
    if year_match:
        year = int(year_match.group(1))

    # Extract month
    for month_name, month_num in MONTH_NAME_TO_NUM.items():
        if month_name in name:
            month = month_num
            break

    return year, month


def _parse_table(table: list[list], year: int, month: int) -> list[dict]:
    """Parse a single extracted table into sales records.

    FCAI tables typically have headers like:
    Make | Model | Segment | Units | Market Share (%)
    """
    if not table or len(table) < 2:
        return []

    # First row is typically the header
    header = [str(cell).strip().lower() if cell else "" for cell in table[0]]

    # Try to identify column positions
    col_map = _identify_columns(header)
    if not col_map:
        return []

    records: list[dict] = []
    for row in table[1:]:
        if not row or all(cell is None or str(cell).strip() == "" for cell in row):
            continue

        record = _extract_row(row, col_map, year, month)
        if record:
            records.append(record)

    return records


def _identify_columns(header: list[str]) -> dict[str, int] | None:
    """Map column names to indices based on header text."""
    col_map: dict[str, int] = {}

    for i, col in enumerate(header):
        col_lower = col.lower().strip()
        if any(kw in col_lower for kw in ("make", "manufacturer", "brand")):
            col_map["make"] = i
        elif any(kw in col_lower for kw in ("model", "nameplate")):
            col_map["model"] = i
        elif any(kw in col_lower for kw in ("segment", "category", "type")):
            col_map["segment"] = i
        elif any(kw in col_lower for kw in ("fuel", "powertrain")):
            col_map["fuel_type"] = i
        elif any(kw in col_lower for kw in ("unit", "sales", "volume", "total")):
            col_map["units_sold"] = i
        elif any(kw in col_lower for kw in ("share", "%", "percent")):
            col_map["market_share"] = i

    # Must have at least a make or units column to be useful
    if "make" not in col_map and "units_sold" not in col_map:
        return None

    return col_map


def _extract_row(
    row: list, col_map: dict[str, int], year: int, month: int
) -> dict | None:
    """Extract a single data row using the column mapping."""

    def _get(key: str) -> str:
        idx = col_map.get(key)
        if idx is None or idx >= len(row):
            return ""
        val = row[idx]
        return str(val).strip() if val is not None else ""

    make = _get("make")
    if not make or make.lower() in ("total", "grand total", ""):
        return None

    units_str = _get("units_sold").replace(",", "").replace(" ", "")
    units = None
    if units_str:
        try:
            units = int(float(units_str))
        except (ValueError, TypeError):
            pass

    share_str = _get("market_share").replace("%", "").replace(",", "").strip()
    share = None
    if share_str:
        try:
            share = float(share_str)
        except (ValueError, TypeError):
            pass

    return {
        "year": year,
        "month": month,
        "make": make,
        "model": _get("model"),
        "segment": _get("segment"),
        "fuel_type": _get("fuel_type"),
        "units_sold": units,
        "market_share": share,
    }
