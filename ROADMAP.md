# Roadmap - Scraper_0061_MotorVehicles

## Completed

- [x] Marklines HTML scraper (fetch, parse, load by make + vehicle type + commentary)
- [x] Marklines browser fallback via Playwright (optional, config toggle)
- [x] FCAI PDF pipeline (download, pdfplumber extraction, load by make/model/segment) -- retired, superseded by articles pipeline
- [x] FCAI article scraper (listing pagination, sales article classification, multi-category backfill)
- [x] Vision LLM table extraction from article images (OpenRouter + Anthropic providers)
- [x] State/territory time-series derived from extracted article tables
- [x] Monthly `update` command with dynamic year scope and skip-processed logic
- [x] Structured Pydantic report models from update (JSON export, `summary_text()`)
- [x] CSV, JSON, and Excel export (`--format csv|json|excel`) -- includes marklines_sales, fcai_extracted_tables, fcai_state_sales
- [x] Database migrations (6 numbered SQL files)
- [x] Core test suite (169 tests: parser, catalog, classification, state sales, update orchestration, quality, notifications)
- [x] Fix Marklines current-year URL routing (current page covers current year; only fetch previous year)
- [x] Marklines incremental mode (content hash comparison via `scrape_runs.content_hash` column)
- [x] FCAI article HTML table fallback (extracts `<table>` elements when no images present)
- [x] Slack notifications via webhook (no-op when `SLACK_WEBHOOK_URL` not set)
- [x] Prefect flow integration (`prefect_flow.py` with cron scheduling, optional dependency)
- [x] Data quality checks (totals cross-check, anomalous counts, state sum validation, duplicate articles)
- [x] Historical coverage expansion: Marklines extended to 2014, FCAI articles back to 2005 via multi-category backfill
- [x] `backfill` command with `--max-pages` and `--categories` options

## Current Data Coverage (as of Feb 2026)

| Source | Range | Records | Notes |
|--------|-------|---------|-------|
| Marklines sales | Feb 2014 - Jan 2026 | 1,678 | By manufacturer, monthly. Gap: Dec 2014 |
| FCAI articles | Sep 2020 - Jan 2026 | 57 | All with extracted tables. ~6 scattered month gaps |
| FCAI state sales | Oct 2024 - Jan 2026 | 126 | 8 states + total. Gap: Aug 2025 |

## Retired

- **FCAI PDF pipeline** (`fcai run`, `fcai download`, `fcai parse`) -- the articles pipeline covers the same data with better state/territory breakdowns. PDF commands remain in CLI for reference but `fcai_sales_data` table has 0 records. No further development planned.

## Parked (future ideas)

### Dashboard / visualization layer
- Expose data via a lightweight API (FastAPI) or static site
- Time-series charts for national + state/territory trends
- Make/model market share analysis
- YoY comparison views

### Multi-country expansion
Marklines has similar pages for other countries. The scraper architecture (config-driven URL patterns, generic table parsing) could be extended to other markets with per-country config sections.
