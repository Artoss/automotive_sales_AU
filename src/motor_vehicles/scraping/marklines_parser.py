"""
Parse Marklines HTML pages for sales data using BeautifulSoup.

Extracts from each monthly section:
- Maker/brand sales (including Others and Total rows) with share and YoY
- Vehicle type breakdown (Passenger Cars, SUV, LCV, HCV, Total)
- Text commentary paragraphs

Page structure:
  <h3><a id="jan"></a>Flash report, January 2026</h3>
  <p>Commentary text...</p>
  <table>Vehicle type table</table>
  <table>Maker/Brand table</table>
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

from bs4 import BeautifulSoup, Tag

logger = logging.getLogger("motor_vehicles.scraping.marklines_parser")

# Month name to number (handles abbreviations and full names)
MONTH_MAP: dict[str, int] = {}
_MONTH_NAMES = [
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
]
for _i, _name in enumerate(_MONTH_NAMES, 1):
    MONTH_MAP[_name] = _i
    MONTH_MAP[_name[:3]] = _i  # jan, feb, ...


@dataclass
class SectionResult:
    """Data extracted from one monthly section."""
    year: int
    month: int
    heading: str = ""
    commentary: str = ""
    maker_sales: list[dict] = field(default_factory=list)
    vehicle_type_sales: list[dict] = field(default_factory=list)


@dataclass
class PageResult:
    """All data extracted from a single Marklines HTML page."""
    source_url: str = ""
    sections: list[SectionResult] = field(default_factory=list)

    @property
    def all_maker_sales(self) -> list[dict]:
        out: list[dict] = []
        for s in self.sections:
            out.extend(s.maker_sales)
        return out

    @property
    def all_vehicle_type_sales(self) -> list[dict]:
        out: list[dict] = []
        for s in self.sections:
            out.extend(s.vehicle_type_sales)
        return out

    @property
    def all_commentary(self) -> list[dict]:
        out: list[dict] = []
        for s in self.sections:
            if s.commentary.strip():
                out.append({
                    "year": s.year,
                    "month": s.month,
                    "report_date": s.heading,
                    "commentary": s.commentary.strip(),
                    "source_url": self.source_url,
                })
        return out


def parse_page(html: str, source_url: str = "") -> PageResult:
    """Parse a full Marklines page into structured data."""
    soup = BeautifulSoup(html, "html.parser")
    result = PageResult(source_url=source_url)

    # Find all section headings (h3 tags with "flash report" text)
    headings = soup.find_all("h3")
    flash_headings = [h for h in headings if _is_flash_heading(h)]

    if not flash_headings:
        # Try to parse as a single-section page (no h3 headings)
        # Look for tables directly
        tables = soup.find_all("table")
        if tables:
            section = _parse_tables_without_heading(tables, source_url)
            if section:
                result.sections.append(section)
        return result

    for heading in flash_headings:
        year, month = _parse_heading_date(heading.get_text())
        if not year or not month:
            continue

        section = SectionResult(
            year=year,
            month=month,
            heading=heading.get_text(strip=True),
        )

        # Collect commentary: all <p> tags between this heading and the next
        # heading or first table
        commentary_parts: list[str] = []
        tables_in_section: list[Tag] = []

        sibling = heading.find_next_sibling()
        while sibling:
            if sibling.name == "h3":
                break
            if sibling.name == "p":
                text = sibling.get_text(strip=True)
                if text:
                    commentary_parts.append(text)
            if sibling.name == "table":
                tables_in_section.append(sibling)
            # Also check for tables inside div wrappers
            if sibling.name == "div":
                for tbl in sibling.find_all("table"):
                    tables_in_section.append(tbl)
            sibling = sibling.find_next_sibling()

        section.commentary = "\n\n".join(commentary_parts)

        # Parse tables in this section
        for table in tables_in_section:
            _classify_and_parse_table(table, section, source_url)

        result.sections.append(section)

    logger.info(
        "Parsed %d sections from %s (sales=%d, vtypes=%d, commentary=%d)",
        len(result.sections),
        source_url,
        len(result.all_maker_sales),
        len(result.all_vehicle_type_sales),
        len(result.all_commentary),
    )
    return result


def _is_flash_heading(tag: Tag) -> bool:
    """Check if an h3 tag is a flash report heading."""
    text = tag.get_text(strip=True).lower()
    return "flash report" in text or "flash" in text


def _parse_heading_date(text: str) -> tuple[int | None, int | None]:
    """Extract year and month from heading text like 'Flash report, January 2026'."""
    text_lower = text.lower().strip()

    year = None
    month = None

    # Extract year
    year_match = re.search(r"\b(20\d{2})\b", text_lower)
    if year_match:
        year = int(year_match.group(1))

    # Extract month
    for name, num in MONTH_MAP.items():
        if len(name) >= 3 and name in text_lower:
            month = num
            break

    return year, month


def _parse_tables_without_heading(
    tables: list[Tag], source_url: str
) -> SectionResult | None:
    """Parse tables when there's no h3 heading (single-section page)."""
    # Try to determine year/month from the table content itself
    section = SectionResult(year=0, month=0)

    for table in tables:
        _classify_and_parse_table(table, section, source_url)

    # If we got maker sales, infer year/month from the first record
    if section.maker_sales:
        section.year = section.maker_sales[0]["year"]
        section.month = section.maker_sales[0]["month"]
        return section
    if section.vehicle_type_sales:
        section.year = section.vehicle_type_sales[0]["year"]
        section.month = section.vehicle_type_sales[0]["month"]
        return section

    return None


def _classify_and_parse_table(
    table: Tag, section: SectionResult, source_url: str
) -> None:
    """Classify a table as vehicle-type or maker, then parse it."""
    headers = _get_table_headers(table)
    headers_text = " ".join(headers).lower()

    if "type" in headers_text and (
        "maker" not in headers_text and "brand" not in headers_text
    ):
        # Vehicle type table
        records = _parse_vehicle_type_table(table, section.year, section.month, source_url)
        section.vehicle_type_sales.extend(records)
    elif "maker" in headers_text or "brand" in headers_text or "make" in headers_text:
        # Maker/brand table
        records = _parse_maker_table(table, source_url)
        section.maker_sales.extend(records)


def _get_table_headers(table: Tag) -> list[str]:
    """Get all header cell text from a table."""
    headers: list[str] = []
    for th in table.find_all("th"):
        headers.append(th.get_text(strip=True))
    return headers


def _parse_maker_table(table: Tag, source_url: str) -> list[dict]:
    """Parse a Maker/Brand table, extracting all rows including Others and Total.

    Returns list of dicts with keys:
        year, month, make, units_sold, market_share,
        units_sold_prev_year, yoy_pct, source_url
    """
    records: list[dict] = []
    rows = table.find_all("tr")
    if len(rows) < 2:
        return records

    # Parse column structure from header rows
    col_info = _parse_maker_column_structure(rows)
    if not col_info:
        return records

    make_col_idx = col_info["make_col"]
    data_columns = col_info["data_columns"]

    # Parse data rows
    for row in rows:
        cells = row.find_all(["td", "th"])
        if len(cells) <= make_col_idx:
            continue

        make = cells[make_col_idx].get_text(strip=True)
        if not make or make.lower() in ("nan", "maker/brand", "-"):
            continue
        # Skip rows where the make cell is just a number (rank column)
        if make.isdigit():
            continue

        for col_spec in data_columns:
            idx = col_spec["idx"]
            if idx >= len(cells):
                continue

            units = _parse_int_cell(cells[idx].get_text(strip=True))
            if units is None:
                continue

            rec: dict = {
                "year": col_spec["year"],
                "month": col_spec["month"],
                "make": make,
                "units_sold": units,
                "source_url": source_url,
                "market_share": None,
                "units_sold_prev_year": None,
                "yoy_pct": None,
            }

            # Look for share column (next column after units)
            share_idx = col_spec.get("share_idx")
            if share_idx is not None and share_idx < len(cells):
                rec["market_share"] = _parse_pct_cell(
                    cells[share_idx].get_text(strip=True)
                )

            # Look for previous year column
            prev_idx = col_spec.get("prev_year_idx")
            if prev_idx is not None and prev_idx < len(cells):
                rec["units_sold_prev_year"] = _parse_int_cell(
                    cells[prev_idx].get_text(strip=True)
                )

            # Look for YoY column
            yoy_idx = col_spec.get("yoy_idx")
            if yoy_idx is not None and yoy_idx < len(cells):
                rec["yoy_pct"] = _parse_pct_cell(
                    cells[yoy_idx].get_text(strip=True)
                )

            records.append(rec)

    return records


def _parse_maker_column_structure(rows: list[Tag]) -> dict | None:
    """Analyze header rows to determine column layout.

    Builds a proper grid accounting for rowspan and colspan, then
    identifies data columns (year+month) and their related columns
    (share, prev year, yoy) using absolute column indices.

    Returns dict with:
        make_col: index of the Maker/Brand column
        data_columns: list of {idx, year, month, share_idx?, prev_year_idx?, yoy_idx?}
    """
    # Collect header <tr> elements
    header_trs: list[Tag] = []
    for row in rows:
        if row.find_all("th"):
            header_trs.append(row)

    if not header_trs:
        return None

    # Build a proper grid accounting for rowspan/colspan
    grid = _build_header_grid(header_trs)
    if not grid:
        return None

    n_cols = len(grid[0])

    # Find make column from the grid
    make_col = None
    for col in range(n_cols):
        for r in range(len(grid)):
            cell_text = (grid[r][col] or "").lower()
            if cell_text in ("maker/brand", "maker", "brand", "make"):
                make_col = col
                break
        if make_col is not None:
            break

    if make_col is None:
        return None

    # Use the last grid row for month labels, first row for year labels
    top_row = grid[0]
    bottom_row = grid[-1]

    # Build year map from the top row
    year_for_col: dict[int, int] = {}
    for col in range(n_cols):
        text = top_row[col] or ""
        year_match = re.search(r"\b(20\d{2})\b", text)
        if year_match:
            year_for_col[col] = int(year_match.group(1))

    # Identify data columns using absolute column indices
    data_columns: list[dict] = []

    for col in range(n_cols):
        cell_text = (bottom_row[col] or "").lower().strip().rstrip(".")
        year = year_for_col.get(col)
        month = _match_month(cell_text)

        if year and month:
            col_spec: dict = {"idx": col, "year": year, "month": month}

            # Look ahead for related columns (share, prev year, yoy)
            for j in range(col + 1, min(col + 6, n_cols)):
                next_text = (bottom_row[j] or "").lower().strip()
                next_year = year_for_col.get(j)

                if "share" in next_text and next_year == year:
                    if "share_idx" not in col_spec:
                        col_spec["share_idx"] = j
                elif (
                    _match_month(next_text.rstrip(".")) == month
                    and next_year
                    and next_year < year
                ):
                    if "prev_year_idx" not in col_spec:
                        col_spec["prev_year_idx"] = j
                elif "y-o-y" in next_text or "yoy" in next_text:
                    if "yoy_idx" not in col_spec:
                        col_spec["yoy_idx"] = j
                elif _match_month(next_text.rstrip(".")):
                    break  # Next month column, stop looking

            data_columns.append(col_spec)

    if not data_columns:
        return None

    return {"make_col": make_col, "data_columns": data_columns}


def _build_header_grid(header_trs: list[Tag]) -> list[list[str | None]]:
    """Build a 2D grid from header rows, accounting for rowspan and colspan.

    Returns a list of rows, each a list of cell text values at each absolute
    column position. Cells spanned by rowspan/colspan are filled with the
    spanning cell's text.
    """
    if not header_trs:
        return []

    n_rows = len(header_trs)
    # Calculate total columns from the row with the most cell spans
    max_cols = 0
    for tr in header_trs:
        cols = sum(
            int(cell.get("colspan", 1)) for cell in tr.find_all(["th", "td"])
        )
        max_cols = max(max_cols, cols)

    # Also account for rowspan expanding into rows with fewer declared cells
    # by computing from the first row which typically spans the full width
    first_row_cols = sum(
        int(cell.get("colspan", 1))
        for cell in header_trs[0].find_all(["th", "td"])
    )
    max_cols = max(max_cols, first_row_cols)

    grid: list[list[str | None]] = [
        [None] * max_cols for _ in range(n_rows)
    ]

    for row_idx, tr in enumerate(header_trs):
        cells = tr.find_all(["th", "td"])
        col_idx = 0
        for cell in cells:
            # Skip columns already occupied by rowspan from previous rows
            while col_idx < max_cols and grid[row_idx][col_idx] is not None:
                col_idx += 1
            if col_idx >= max_cols:
                break

            text = cell.get_text(strip=True)
            rs = int(cell.get("rowspan", 1))
            cs = int(cell.get("colspan", 1))

            for dr in range(rs):
                for dc in range(cs):
                    r = row_idx + dr
                    c = col_idx + dc
                    if r < n_rows and c < max_cols:
                        grid[r][c] = text

            col_idx += cs

    return grid


def _match_month(text: str) -> int | None:
    """Match month name/abbreviation to month number."""
    text = text.lower().strip().rstrip(".")
    # Don't match ranges like "jan-dec" or "jan.-may"
    if "-" in text:
        return None
    for name, num in MONTH_MAP.items():
        if len(name) >= 3 and text.startswith(name):
            return num
    return None


def _parse_flat_column_header(text: str) -> tuple[int | None, int | None]:
    """Parse a flat column header like 'Jan. 2024' into (year, month)."""
    text_lower = text.lower().strip()

    # Skip share/yoy/ytd
    if "share" in text_lower or "y-o-y" in text_lower or "yoy" in text_lower:
        return None, None
    if re.search(r"jan\w*\.?\s*-\s*[a-z]", text_lower):
        return None, None

    year = None
    month = None

    year_match = re.search(r"\b(20\d{2})\b", text_lower)
    if year_match:
        year = int(year_match.group(1))

    for name, num in MONTH_MAP.items():
        if len(name) >= 3 and re.search(rf"\b{name}\.?\b", text_lower):
            month = num
            break

    if year and month:
        return year, month
    return None, None


def _parse_vehicle_type_table(
    table: Tag, year: int, month: int, source_url: str
) -> list[dict]:
    """Parse a vehicle type summary table.

    These tables typically have columns like:
        Type | Current Month | Prev Year Month | YoY%
    Returns list of dicts with vehicle_type, units_sold, etc.
    """
    records: list[dict] = []
    rows = table.find_all("tr")
    if len(rows) < 2:
        return records

    # Get header row to identify columns
    header_cells = rows[0].find_all(["th", "td"])
    headers = [c.get_text(strip=True).lower() for c in header_cells]

    # Find column indices
    type_col = None
    current_col = None
    prev_col = None
    yoy_col = None

    for i, h in enumerate(headers):
        if "type" in h:
            type_col = i
        elif any(kw in h for kw in ("share", "y-o-y", "yoy")):
            if "y-o-y" in h or "yoy" in h:
                yoy_col = i
        else:
            # Numeric month columns - first is current, second is prev year
            month_match = _match_month(h.rstrip("."))
            year_match = re.search(r"\b(20\d{2})\b", h)
            if month_match or year_match:
                if current_col is None:
                    current_col = i
                elif prev_col is None:
                    prev_col = i

    if type_col is None:
        # Try first column as type
        type_col = 0
        if current_col is None and len(headers) > 1:
            current_col = 1
        if prev_col is None and len(headers) > 2:
            prev_col = 2

    for row in rows[1:]:
        cells = row.find_all(["td", "th"])
        if len(cells) <= type_col:
            continue

        vehicle_type = cells[type_col].get_text(strip=True)
        if not vehicle_type or vehicle_type.lower() in ("type", "-", "nan"):
            continue

        rec = {
            "year": year,
            "month": month,
            "vehicle_type": vehicle_type,
            "units_sold": None,
            "units_sold_prev_year": None,
            "yoy_pct": None,
            "source_url": source_url,
        }

        if current_col is not None and current_col < len(cells):
            rec["units_sold"] = _parse_int_cell(cells[current_col].get_text(strip=True))

        if prev_col is not None and prev_col < len(cells):
            rec["units_sold_prev_year"] = _parse_int_cell(
                cells[prev_col].get_text(strip=True)
            )

        if yoy_col is not None and yoy_col < len(cells):
            rec["yoy_pct"] = _parse_pct_cell(cells[yoy_col].get_text(strip=True))

        records.append(rec)

    return records


def _parse_int_cell(text: str) -> int | None:
    """Parse a table cell into an integer (handles commas, spaces)."""
    s = text.strip().replace(",", "").replace(" ", "").replace("\u3000", "")
    if not s or s.lower() in ("nan", "-", "n/a", ""):
        return None
    if "%" in s:
        return None
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return None


def _parse_pct_cell(text: str) -> float | None:
    """Parse a percentage cell like '20.5%' into a float."""
    s = text.strip().replace("%", "").replace(",", "").replace(" ", "")
    if not s or s.lower() in ("nan", "-", "n/a", ""):
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None
