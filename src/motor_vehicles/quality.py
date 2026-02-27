"""Data quality checks for the monthly update pipeline.

Pure functions that inspect database state and return structured findings.
Designed to run after the update steps, before final reporting.
"""

from __future__ import annotations

import logging

from pydantic import BaseModel, Field

logger = logging.getLogger("motor_vehicles.quality")


class QualityIssue(BaseModel):
    """A single data quality finding."""
    check: str
    severity: str  # "warning" or "error"
    message: str
    detail: str = ""


class QualityReport(BaseModel):
    """Aggregated results from all quality checks."""
    issues: list[QualityIssue] = Field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        return any(i.severity == "error" for i in self.issues)

    @property
    def has_warnings(self) -> bool:
        return any(i.severity == "warning" for i in self.issues)

    def summary_text(self) -> str:
        if not self.issues:
            return "Quality checks: all passed"
        lines = ["--- Quality Checks ---"]
        for issue in self.issues:
            icon = "ERROR" if issue.severity == "error" else "WARN"
            lines.append(f"  [{icon}] {issue.check}: {issue.message}")
            if issue.detail:
                lines.append(f"         {issue.detail}")
        return "\n".join(lines)


def run_quality_checks(db) -> QualityReport:
    """Run all quality checks against current database state.

    Args:
        db: Connected Database instance.

    Returns:
        QualityReport with any issues found.
    """
    report = QualityReport()

    _check_marklines_totals(db, report)
    _check_monthly_record_counts(db, report)
    _check_state_sales_vs_total(db, report)
    _check_duplicate_articles(db, report)

    if report.issues:
        logger.warning("Quality checks found %d issues", len(report.issues))
    else:
        logger.info("Quality checks: all passed")

    return report


def _check_marklines_totals(db, report: QualityReport) -> None:
    """Cross-check Marklines Total row against sum of individual makes."""
    with db.cursor() as cur:
        cur.execute("""
            WITH make_sums AS (
                SELECT year, month, SUM(units_sold) as sum_units
                FROM marklines_sales
                WHERE make NOT IN ('Total', 'Others')
                    AND units_sold IS NOT NULL
                GROUP BY year, month
            ),
            totals AS (
                SELECT year, month, units_sold as total_units
                FROM marklines_sales
                WHERE make = 'Total'
            )
            SELECT t.year, t.month, t.total_units, m.sum_units,
                   t.total_units - m.sum_units as diff
            FROM totals t
            JOIN make_sums m ON t.year = m.year AND t.month = m.month
            WHERE t.total_units IS NOT NULL
                AND m.sum_units IS NOT NULL
                AND ABS(t.total_units - m.sum_units) > t.total_units * 0.01
            ORDER BY t.year DESC, t.month DESC
            LIMIT 5
        """)
        rows = cur.fetchall()

    for row in rows:
        report.issues.append(QualityIssue(
            check="marklines_totals",
            severity="warning",
            message=(
                f"{row['year']}/{row['month']:02d}: "
                f"Total ({row['total_units']:,}) differs from "
                f"sum of makes ({row['sum_units']:,}) by {row['diff']:,}"
            ),
        ))


def _check_monthly_record_counts(db, report: QualityReport) -> None:
    """Flag months with unusually low record counts vs the median."""
    with db.cursor() as cur:
        # Get per-month make counts
        cur.execute("""
            SELECT year, month, COUNT(*) as cnt
            FROM marklines_sales
            WHERE make NOT IN ('Total', 'Others')
            GROUP BY year, month
            ORDER BY year, month
        """)
        rows = cur.fetchall()

    if len(rows) < 6:
        return  # Not enough data for statistical comparison

    counts = [r["cnt"] for r in rows]
    counts.sort()
    median = counts[len(counts) // 2]
    threshold = median * 0.5  # Less than half the median is suspicious

    for row in rows:
        if row["cnt"] < threshold:
            report.issues.append(QualityIssue(
                check="low_record_count",
                severity="warning",
                message=(
                    f"{row['year']}/{row['month']:02d}: "
                    f"only {row['cnt']} makes (median is {median})"
                ),
            ))


def _check_state_sales_vs_total(db, report: QualityReport) -> None:
    """Validate state sales sum against the Total row for each month."""
    with db.cursor() as cur:
        cur.execute("""
            SELECT year, month,
                SUM(CASE WHEN state_abbrev != 'TOTAL' THEN units_sold ELSE 0 END) as state_sum,
                MAX(CASE WHEN state_abbrev = 'TOTAL' THEN units_sold END) as total_row
            FROM fcai_state_sales
            GROUP BY year, month
            HAVING MAX(CASE WHEN state_abbrev = 'TOTAL' THEN units_sold END) IS NOT NULL
            ORDER BY year DESC, month DESC
        """)
        rows = cur.fetchall()

    for row in rows:
        state_sum = row["state_sum"] or 0
        total_row = row["total_row"]
        if total_row and abs(state_sum - total_row) > total_row * 0.02:
            diff = state_sum - total_row
            report.issues.append(QualityIssue(
                check="state_sales_total",
                severity="warning",
                message=(
                    f"{row['year']}/{row['month']:02d}: "
                    f"state sum ({state_sum:,}) differs from "
                    f"Total row ({total_row:,}) by {diff:+,}"
                ),
            ))


def _check_duplicate_articles(db, report: QualityReport) -> None:
    """Detect articles covering the same year/month (potential duplicates)."""
    with db.cursor() as cur:
        cur.execute("""
            SELECT year, month, COUNT(*) as cnt,
                   array_agg(slug) as slugs
            FROM fcai_articles
            WHERE is_sales_article = TRUE
                AND year IS NOT NULL AND month IS NOT NULL
            GROUP BY year, month
            HAVING COUNT(*) > 1
            ORDER BY year DESC, month DESC
        """)
        rows = cur.fetchall()

    for row in rows:
        slugs = row["slugs"]
        if isinstance(slugs, list):
            slug_str = ", ".join(slugs[:3])
        else:
            slug_str = str(slugs)
        report.issues.append(QualityIssue(
            check="duplicate_articles",
            severity="warning",
            message=(
                f"{row['year']}/{row['month']:02d}: "
                f"{row['cnt']} articles for same month"
            ),
            detail=slug_str,
        ))
