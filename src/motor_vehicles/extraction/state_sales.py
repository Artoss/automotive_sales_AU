"""
Extract State/Territory sales time-series from FCAI article extracted tables.

Handles two table formats produced by Vision LLM extraction:
- Clean 4-column: [State/Territory, Mon-YY, Mon-YY(prev), % diff]
- Merged 9-column: [Rank, Vehicle, Mon-YY, Mon-YY(prev), % diff,
                     State/Territory, Mon-YY, Mon-YY(prev), % diff]
  (side-by-side Top 10 Models + State/Territory in one table)
"""

from __future__ import annotations

import json
import logging
import re

logger = logging.getLogger("motor_vehicles.extraction.state_sales")

STATE_NAMES = {
    "australian capital territory": "ACT",
    "new south wales": "NSW",
    "northern territory": "NT",
    "queensland": "QLD",
    "south australia": "SA",
    "tasmania": "TAS",
    "victoria": "VIC",
    "western australia": "WA",
}


def parse_int(value: str) -> int | None:
    """Parse a string like '1,234' or '1234' into an int."""
    if not value or not value.strip():
        return None
    cleaned = value.strip().replace(",", "").replace(" ", "")
    try:
        return int(cleaned)
    except ValueError:
        return None


def parse_float(value: str) -> float | None:
    """Parse a string like '-3.5' or '2.0%' into a float."""
    if not value or not value.strip():
        return None
    cleaned = value.strip().replace("%", "").replace(",", "").replace(" ", "")
    try:
        return float(cleaned)
    except ValueError:
        return None


def extract_state_sales(
    headers: list,
    row_data: list,
    year: int,
    month: int,
) -> list[dict]:
    """Extract State/Territory records from a single extracted table.

    Returns a list of dicts with keys:
        year, month, state, state_abbrev, units_sold,
        units_sold_prev_year, yoy_pct
    """
    if isinstance(headers, str):
        headers = json.loads(headers)
    if isinstance(row_data, str):
        row_data = json.loads(row_data)

    # Determine table format
    if not headers or not row_data:
        return []

    n_cols = len(headers)
    state_col_offset = _find_state_column(headers, row_data)
    if state_col_offset is None:
        return []

    results = []
    for row in row_data:
        if len(row) <= state_col_offset:
            continue

        state_raw = str(row[state_col_offset]).strip()
        state_lower = state_raw.lower()

        if state_lower not in STATE_NAMES and state_lower != "total":
            continue

        abbrev = STATE_NAMES.get(state_lower, "TOTAL")

        # Values are in the 3 columns after the state name
        units_current = parse_int(row[state_col_offset + 1]) if len(row) > state_col_offset + 1 else None
        units_prev = parse_int(row[state_col_offset + 2]) if len(row) > state_col_offset + 2 else None
        yoy_pct = parse_float(row[state_col_offset + 3]) if len(row) > state_col_offset + 3 else None

        if units_current is None:
            continue

        results.append({
            "year": year,
            "month": month,
            "state": state_raw.title(),
            "state_abbrev": abbrev,
            "units_sold": units_current,
            "units_sold_prev_year": units_prev,
            "yoy_pct": yoy_pct,
        })

    if results:
        logger.info(
            "Extracted %d state records for %d/%02d",
            len(results), year, month,
        )

    return results


def _find_state_column(headers: list, row_data: list) -> int | None:
    """Find which column index contains State/Territory names.

    Returns the column offset for the state name, or None.
    """
    # Check header names first
    for idx, h in enumerate(headers):
        if "state" in str(h).lower() or "territory" in str(h).lower():
            return idx

    # Fall back: check row data for state names in various columns
    state_set = set(STATE_NAMES.keys()) | {"total"}
    for check_col in (0, 5, 6):
        matches = 0
        for row in row_data:
            if len(row) > check_col and str(row[check_col]).strip().lower() in state_set:
                matches += 1
        if matches >= 3:
            return check_col

    return None
