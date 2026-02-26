# Developer Projects - Shared Conventions

## Tech Stack

- **Python 3.12** managed with **uv** (package installer + runner)
- **hatchling** build backend
- **Pydantic v2** for config validation and data models
- **Click** for CLI entry points
- **psycopg 3** with `dict_row` factory for PostgreSQL
- **httpx** for HTTP clients (async-capable, preferred over requests)
- **tenacity** for retry/backoff on HTTP calls
- **pandas** for tabular data manipulation
- **pyyaml** + **python-dotenv** for config loading

## Project Structure Template

```
Scraper_NNNN_Name/
  src/<package_name>/
    __init__.py
    config.py              # AppConfig (Pydantic v2) + YAML loader
    main.py                # Click CLI entry point
    scraping/              # HTTP clients, page fetchers
    extraction/            # Data extraction (PDF, HTML parsing)
    storage/
      __init__.py
      database.py          # PostgreSQL connection + cursor context manager
      models.py            # Pydantic models for DB entities
      loader.py            # Orchestrate parse + load to DB
    utils/
      __init__.py
      logging.py           # Rotating file + console logging
  tests/
  migrations/              # Numbered SQL files (001_*, 002_*, ...)
  config.yaml
  .env.example
  pyproject.toml
```

## Config Pattern

- **config.yaml** holds all non-secret settings (scope, HTTP, database, logging, export)
- **.env** holds secrets (PGPASSWORD etc.) loaded via python-dotenv
- **AppConfig** (Pydantic BaseModel) validates and merges both sources
- Environment variables override YAML: PGUSER, PGPASSWORD, PGHOST, PGPORT, PGDATABASE

## Database Conventions

- PostgreSQL with **psycopg 3** (`psycopg.connect(..., row_factory=dict_row, autocommit=False)`)
- `Database` class with `connect()`, `close()`, `cursor()` context manager (commit/rollback)
- Numbered migrations in `migrations/` directory, applied via `ensure_schema()`
- **Upsert pattern**: `INSERT ... ON CONFLICT (...) DO UPDATE SET ...`
- Every project has a `scrape_runs` table for tracking run history
- Tables use `SERIAL PRIMARY KEY`, `TIMESTAMPTZ DEFAULT NOW()`, `TEXT` for strings

## CLI Pattern

- Click group with `@click.group(invoke_without_command=True)`
- Standard subcommands: `migrate`, `run`, `download`, `parse`, `status`, `export`
- Config loaded in group, passed via `ctx.obj["config"]`
- Entry point in pyproject.toml: `project-name = "package.main:cli"`

## Coding Conventions

- ASCII-only strings in Click docstrings (no smart quotes/em-dashes)
- `from __future__ import annotations` in all modules
- Pydantic models for all DB entities
- `logging.getLogger("package.module")` pattern
- pytest for tests
- Lazy imports in CLI command implementations (keep startup fast)

## Reference Projects

| Project | Key Patterns |
|---------|-------------|
| Scraper_0060_PHIDU_SocialHealthAtlas | Config, DB, CLI, models, logging - canonical reference |
| Scraper_0057_PDF_Extractor | PDF table extraction with pdfplumber |
| Scraper_0055, 0056 | Earlier scraper patterns |
