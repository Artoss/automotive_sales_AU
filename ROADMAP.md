# Roadmap - Scraper_0061_MotorVehicles

## Completed

- [x] Marklines HTML scraper (fetch, parse, load by make + vehicle type + commentary)
- [x] Marklines browser fallback via Playwright (optional, config toggle)
- [x] FCAI PDF pipeline (download, pdfplumber extraction, load by make/model/segment)
- [x] FCAI article scraper (listing pagination, sales article classification)
- [x] Vision LLM table extraction from article images (OpenRouter + Anthropic providers)
- [x] State/territory time-series derived from extracted article tables
- [x] Monthly `update` command with dynamic year scope and skip-processed logic
- [x] Structured Pydantic report models from update (JSON export, `summary_text()`)
- [x] CSV, JSON, and Excel export (`--format csv|json|excel`)
- [x] Database migrations (5 numbered SQL files)
- [x] Core test suite (parser, catalog, PDF extraction, live DB integration)
- [x] Fix Marklines current-year URL routing (current page covers current year; only fetch previous year)
- [x] FCAI PDF incremental mode (`--mode incremental` skips unchanged files by hash comparison)
- [x] Tests for update module, state sales extraction, and article classification (63 new tests)
- [x] Slack notifications via webhook (no-op when `SLACK_WEBHOOK_URL` not set)
- [x] Prefect flow integration (`prefect_flow.py` with cron scheduling, optional dependency)
- [x] Data quality checks (totals cross-check, anomalous counts, state sum validation, duplicate articles)
- [x] FCAI article HTML table fallback (extracts `<table>` elements when no images present)
- [x] Tests for quality checks, notifications, and HTML table extraction (19 new tests, 123 total)

## Short-term

### Incremental mode for Marklines
FCAI PDF incremental mode is implemented. Marklines could also skip re-parsing if page content hash matches previous run. Lower priority since Marklines pages are small and upserts are idempotent.

### Mock-based integration tests
Current tests cover pure functions (parsing, classification, report models). Add mock-based tests that verify the update step orchestration (mock DB + HTTP, verify report counts and error handling).

## Long-term

### Historical coverage expansion
- Extend Marklines scraping to pre-2018 if data is available
- Backfill FCAI articles beyond current listing pagination depth
- Consolidate PDF and article pipelines into a unified FCAI time-series

### Dashboard / visualization layer
- Expose data via a lightweight API (FastAPI) or static site
- Time-series charts for national + state/territory trends
- Make/model market share analysis
- YoY comparison views

### Multi-country expansion
Marklines has similar pages for other countries. The scraper architecture (config-driven URL patterns, generic table parsing) could be extended to other markets with per-country config sections.
