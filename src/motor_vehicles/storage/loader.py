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
    vehicle_type_records: list[dict],
    commentary_records: list[dict],
) -> int:
    """Load parsed Marklines data into the database. Returns total record count."""
    sales_count = db.upsert_marklines_sales(sales_records, run_id)
    vtype_count = db.upsert_marklines_vehicle_types(vehicle_type_records, run_id)
    commentary_count = db.upsert_marklines_commentary(commentary_records, run_id)
    total = sales_count + vtype_count + commentary_count
    logger.info(
        "Loaded %d marklines records (sales=%d, vtypes=%d, commentary=%d) for run #%d",
        total, sales_count, vtype_count, commentary_count, run_id,
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


def load_fcai_article(
    db: Database,
    run_id: int,
    article: dict,
    images: list[dict],
    extracted_tables: dict[int, list[dict]],
) -> int:
    """Load an FCAI article, its images, and extracted tables.

    Args:
        db: Database instance
        run_id: Current scrape run ID
        article: Article dict with url, slug, title, etc.
        images: List of image dicts with image_url, image_filename, local_path, etc.
        extracted_tables: Mapping from image index to list of extracted table dicts.

    Returns:
        Total number of extracted table records inserted.
    """
    article_id = db.upsert_fcai_article(article, run_id)

    total_tables = 0
    for idx, img in enumerate(images):
        image_id = db.upsert_fcai_article_image(article_id, img)

        tables = extracted_tables.get(idx, [])
        for table in tables:
            db.insert_fcai_extracted_table(image_id, table)
            total_tables += 1

    logger.info(
        "Loaded FCAI article #%d with %d images, %d tables for run #%d",
        article_id, len(images), total_tables, run_id,
    )
    return total_tables
