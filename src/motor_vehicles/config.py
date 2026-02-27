"""
Configuration loader and validation.
Reads config.yaml + .env and produces a strongly-typed AppConfig object.
Environment variables override YAML values for sensitive fields.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Literal

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field, model_validator


# ---------------------------------------------------------------------------
# Sub-configs
# ---------------------------------------------------------------------------

class MarklinesConfig(BaseModel):
    """Marklines scraping scope."""
    base_url: str = "https://www.marklines.com/en/statistics/flash_sales/automotive-sales-in-australia-by-month"
    historical_url_template: str = "https://www.marklines.com/en/statistics/flash_sales/salesfig_australia_{year}"
    recent_years: list[int] = [2025, 2024, 2023, 2022, 2021]
    historical_years: list[int] = [2020, 2019, 2018]
    use_browser_fallback: bool = False


class FcaiArticlesConfig(BaseModel):
    """FCAI article-based scraping scope."""
    listing_url: str = "https://www.fcai.com.au/news-and-media/"
    listing_params: dict[str, str] = {"_sft_category": "media-release"}
    backfill_categories: list[str] = Field(
        default=["media-release", "news"],
        description="Categories to search during backfill (news has historical articles back to 2005)",
    )
    max_pages: int = 5
    image_download_dir: str = "./data/fcai_images"


class FcaiConfig(BaseModel):
    """FCAI scraping scope."""
    base_url: str = "https://www.fcai.com.au/library/publication/"
    filename_template: str = "{month}_{year}_vfacts_media_release_and_industry_summary.pdf"
    years: list[int] = [2024, 2023, 2022]
    months: list[str] = [
        "january", "february", "march", "april", "may", "june",
        "july", "august", "september", "october", "november", "december",
    ]
    download_dir: str = "./data/pdfs"
    articles: FcaiArticlesConfig = Field(default_factory=FcaiArticlesConfig)


class HttpConfig(BaseModel):
    """Rate limiting and HTTP client settings."""
    min_delay_seconds: float = 2.0
    max_delay_seconds: float = 5.0
    max_retries: int = 3
    retry_backoff_factor: float = 2.0
    timeout_seconds: int = 60
    user_agents: list[str] = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ]
    default_headers: dict[str, str] = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-AU,en;q=0.9",
    }


class DatabaseConfig(BaseModel):
    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_database: str = "automotive_sales_au"
    pg_user: str = "postgres"
    pg_password: str = ""

    @model_validator(mode="after")
    def apply_env_overrides(self):
        if env_user := os.getenv("PGUSER"):
            self.pg_user = env_user
        if env_pass := os.getenv("PGPASSWORD"):
            self.pg_password = env_pass
        if env_host := os.getenv("PGHOST"):
            self.pg_host = env_host
        if env_port := os.getenv("PGPORT"):
            self.pg_port = int(env_port)
        if env_db := os.getenv("PGDATABASE"):
            self.pg_database = env_db
        return self

    @property
    def connection_string(self) -> str:
        return (
            f"postgresql://{self.pg_user}:{self.pg_password}"
            f"@{self.pg_host}:{self.pg_port}/{self.pg_database}"
        )

    @property
    def connection_params(self) -> dict:
        return {
            "host": self.pg_host,
            "port": self.pg_port,
            "dbname": self.pg_database,
            "user": self.pg_user,
            "password": self.pg_password,
        }


class VisionConfig(BaseModel):
    """Vision LLM settings for image table extraction."""
    provider: Literal["openrouter", "anthropic"] = "openrouter"
    model: str = "anthropic/claude-sonnet-4"
    api_base_url: str = "https://openrouter.ai/api/v1/chat/completions"
    api_key: str = ""
    max_tokens: int = 16384
    timeout_seconds: int = 120

    @model_validator(mode="after")
    def apply_env_overrides(self):
        if self.provider == "openrouter":
            if env_key := os.getenv("OPENROUTER_API_KEY"):
                self.api_key = env_key
        else:
            if env_key := os.getenv("ANTHROPIC_API_KEY"):
                self.api_key = env_key
        return self


class ExportConfig(BaseModel):
    enabled: bool = True
    format: Literal["csv", "json", "excel"] = "csv"
    output_dir: str = "./exports"
    timestamp_files: bool = True


class LoggingConfig(BaseModel):
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    file: str = "./logs/motor_vehicles.log"
    console: bool = True
    rotate_mb: int = 10
    keep_backups: int = 5


# ---------------------------------------------------------------------------
# Root config
# ---------------------------------------------------------------------------

class AppConfig(BaseModel):
    """Root configuration model."""
    run_mode: Literal["full", "incremental"] = "full"
    marklines: MarklinesConfig = Field(default_factory=MarklinesConfig)
    fcai: FcaiConfig = Field(default_factory=FcaiConfig)
    http: HttpConfig = Field(default_factory=HttpConfig)
    vision: VisionConfig = Field(default_factory=VisionConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    export: ExportConfig = Field(default_factory=ExportConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)

    def config_hash(self) -> str:
        """SHA256 hash of config for run tracking."""
        data = self.model_dump_json(indent=None)
        return hashlib.sha256(data.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

def load_config(
    config_path: str | Path = "config.yaml",
    env_path: str | Path = ".env",
) -> AppConfig:
    """Load YAML + .env, priority: env vars > YAML > defaults."""
    env_file = Path(env_path)
    if env_file.exists():
        load_dotenv(env_file)

    config_file = Path(config_path)
    if config_file.exists():
        with open(config_file, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
    else:
        raw = {}

    config = AppConfig(**raw)

    # Ensure output directories exist
    Path(config.export.output_dir).mkdir(parents=True, exist_ok=True)
    Path(config.logging.file).parent.mkdir(parents=True, exist_ok=True)
    Path(config.fcai.download_dir).mkdir(parents=True, exist_ok=True)
    Path(config.fcai.articles.image_download_dir).mkdir(parents=True, exist_ok=True)

    return config
