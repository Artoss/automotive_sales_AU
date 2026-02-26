"""
Orchestrate parsing and loading data into the database.
"""

from __future__ import annotations

import logging

from motor_vehicles.storage.database import Database

logger = logging.getLogger("motor_vehicles.storage.loader")


def load_marklines_data(
    db: Database,
    run_id: int,
    sales_records: list[dict],
    total_records: list[dict],
) -> int:
    """Load parsed Marklines data into the database. Returns total record count."""
    sales_count = db.upsert_marklines_sales(sales_records, run_id)
    totals_count = db.upsert_marklines_totals(total_records, run_id)
    total = sales_count + totals_count
    logger.info(
        "Loaded %d marklines records (sales=%d, totals=%d) for run #%d",
        total, sales_count, totals_count, run_id,
    )
    return total


def load_fcai_publication(
    db: Database,
    run_id: int,
    publication: dict,
    sales_records: list[dict],
) -> int:
    """Load an FCAI publication and its extracted sales data. Returns record count."""
    pub_id = db.upsert_fcai_publication(publication, run_id)
    count = db.upsert_fcai_sales(sales_records, pub_id)
    if count > 0:
        db.mark_publication_parsed(pub_id)
    logger.info(
        "Loaded FCAI publication #%d with %d sales records for run #%d",
        pub_id, count, run_id,
    )
    return count
