"""Prefect-orchestrated monthly pipeline for Motor Vehicles scraper.

Wraps run_monthly_update() as a Prefect flow with individual tasks
for each step. Sends Slack notifications on success/failure.

Usage:
    uv run python -m motor_vehicles.prefect_flow              # One-shot run
    uv run python -m motor_vehicles.prefect_flow --serve      # Cron scheduler
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from urllib.error import URLError
from urllib.request import urlopen

from dotenv import load_dotenv

load_dotenv()

from prefect import flow, task
from prefect.cache_policies import NONE

from motor_vehicles.config import AppConfig, load_config
from motor_vehicles.notify import notify_update_failure, notify_update_success
from motor_vehicles.update import (
    UpdateReport,
    run_fcai_articles_update,
    run_marklines_update,
    run_state_sales_update,
)
from motor_vehicles.utils.logging import setup_logging

logger = logging.getLogger("motor_vehicles.prefect_flow")

# Default schedule: 5th of each month at 8am AEST
MONTHLY_CRON = os.environ.get("UPDATE_CRON", "0 8 5 * *")


# ---------------------------------------------------------------------------
# Infrastructure readiness checks
# ---------------------------------------------------------------------------

def _wait_for_prefect_api(timeout: int = 120) -> None:
    """Block until the Prefect API server is reachable."""
    api_url = os.environ.get("PREFECT_API_URL", "")
    if not api_url:
        print("PREFECT_API_URL not set -- running in local mode, skipping API check.")
        return

    health_url = api_url.rstrip("/") + "/health"
    print(f"Waiting for Prefect API at {health_url}...")

    deadline = time.time() + timeout
    delay = 1
    while time.time() < deadline:
        try:
            resp = urlopen(health_url, timeout=5)
            if resp.status == 200:
                print("Prefect API is ready.")
                return
        except (URLError, OSError):
            pass
        print(f"  not ready -- retrying in {delay}s...")
        time.sleep(delay)
        delay = min(delay * 2, 10)

    print(f"WARNING: Prefect API not reachable after {timeout}s -- continuing anyway.")


# ---------------------------------------------------------------------------
# Tasks -- thin wrappers around update step functions
# ---------------------------------------------------------------------------

@task(name="marklines", retries=1, retry_delay_seconds=60, cache_policy=NONE)
def task_marklines(config: AppConfig) -> dict:
    """Fetch and load Marklines data for current + previous year."""
    report = run_marklines_update(config)
    return report.model_dump()


@task(name="fcai-articles", retries=1, retry_delay_seconds=60, cache_policy=NONE)
def task_fcai_articles(config: AppConfig) -> dict:
    """Fetch new FCAI articles and extract tables via Vision LLM."""
    report = run_fcai_articles_update(config)
    return report.model_dump()


@task(name="state-sales", retries=1, retry_delay_seconds=30, cache_policy=NONE)
def task_state_sales(config: AppConfig) -> dict:
    """Re-extract state/territory sales from all article tables."""
    report = run_state_sales_update(config)
    return report.model_dump()


@task(name="quality-checks", retries=0, cache_policy=NONE)
def task_quality_checks(config: AppConfig) -> list[dict]:
    """Run data quality checks against current database state."""
    from motor_vehicles.quality import run_quality_checks
    from motor_vehicles.storage.database import Database

    db = Database(config.database)
    try:
        db.connect()
        qr = run_quality_checks(db)
        return [issue.model_dump() for issue in qr.issues]
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="motor-vehicles-monthly-update", log_prints=True)
def monthly_update_flow(config_path: str = "config.yaml") -> None:
    """Full monthly update: marklines -> fcai articles -> state sales -> quality."""
    config = load_config(config_path)
    setup_logging(config.logging)
    current_step = "init"

    try:
        results = {}

        current_step = "marklines"
        results["marklines"] = task_marklines(config)

        current_step = "fcai_articles"
        results["fcai_articles"] = task_fcai_articles(config)

        current_step = "state_sales"
        results["state_sales"] = task_state_sales(config)

        current_step = "quality"
        results["quality_issues"] = task_quality_checks(config)

        # Build summary for notification
        lines = [
            "Marklines: {pages_fetched} pages, {total_records} records".format(
                **results["marklines"]
            ),
            "FCAI Articles: {articles_found} found, {articles_new} new, {tables_extracted} tables".format(
                **results["fcai_articles"]
            ),
            "State Sales: {months_found} months, {records_upserted} records".format(
                **results["state_sales"]
            ),
        ]
        n_issues = len(results["quality_issues"])
        if n_issues:
            lines.append(f"Quality: {n_issues} issue(s)")

        notify_update_success("\n".join(lines))
        logger.info("Monthly update flow complete")

    except Exception as exc:
        notify_update_failure(exc, step=current_step)
        logger.error("Flow failed at step '%s': %s", current_step, exc)
        raise


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Motor Vehicles monthly update (Prefect-orchestrated)."
    )
    parser.add_argument(
        "--serve",
        action="store_true",
        help="Start Prefect scheduler (cron). Otherwise runs once.",
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to config.yaml",
    )
    args = parser.parse_args()

    # Infrastructure readiness
    _wait_for_prefect_api()

    if args.serve:
        monthly_update_flow.serve(
            name="motor-vehicles-monthly",
            cron=MONTHLY_CRON,
        )
    else:
        monthly_update_flow(config_path=args.config)
