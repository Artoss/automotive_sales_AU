"""
FCAI publication URL builder.
Generates a catalog of PDF URLs for year/month combinations.
"""

from __future__ import annotations

import logging

from motor_vehicles.config import FcaiConfig

logger = logging.getLogger("motor_vehicles.scraping.fcai_catalog")

MONTH_NAMES = [
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
]

MONTH_NAME_TO_NUM = {name: i + 1 for i, name in enumerate(MONTH_NAMES)}


def build_catalog(
    config: FcaiConfig,
    year: int | None = None,
    month: str | None = None,
) -> list[dict]:
    """Build a list of FCAI publication entries to download.

    Each entry has: year, month_name, month_num, filename, url.
    Optionally filter to a single year and/or month.
    """
    years = [year] if year else config.years
    months = [month.lower()] if month else config.months

    catalog: list[dict] = []
    for y in years:
        for m in months:
            m_lower = m.lower()
            if m_lower not in MONTH_NAME_TO_NUM:
                logger.warning("Unknown month name: %s", m)
                continue

            filename = config.filename_template.format(month=m_lower, year=y)
            url = config.base_url + filename

            catalog.append({
                "year": y,
                "month_name": m_lower,
                "month_num": MONTH_NAME_TO_NUM[m_lower],
                "filename": filename,
                "url": url,
            })

    logger.info("Built FCAI catalog with %d entries", len(catalog))
    return catalog
