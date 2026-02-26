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

## Short-term

### Incremental mode for Marklines
FCAI PDF incremental mode is implemented. Marklines could also skip re-parsing if page content hash matches previous run. Lower priority since Marklines pages are small and upserts are idempotent.

### Mock-based integration tests
Current tests cover pure functions (parsing, classification, report models). Add mock-based tests that verify the update step orchestration (mock DB + HTTP, verify report counts and error handling).

## Medium-term

### Prefect flow integration
`run_monthly_update()` returns a Pydantic `UpdateReport` by design. Wrap in a Prefect flow:
- Schedule monthly run (e.g. 5th of each month)
- Each step as a Prefect task for observability
- Retry/alert on step failures
- Store run artifacts in Prefect

### Slack notifications
Post `report.summary_text()` to a Slack channel after each update:
- Success: summary with record counts and coverage
- Failure: error details with step that failed
- Coverage gap alerts when new gaps appear

### Data quality checks
Add validation between extraction and loading:
- Flag months with unusually low/high record counts vs historical average
- Cross-check Marklines totals against sum of make-level records
- Detect duplicate or near-duplicate articles
- Validate state sales sum against national total from same article

### FCAI article image-free extraction
Some newer FCAI articles have no embedded images (text-only releases). Investigate:
- Whether table data is available in article HTML/text
- Fallback extraction from article body when no images present

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
