"""
HTTP client for fetching Marklines HTML pages.
Uses httpx with tenacity retry/backoff.

URL patterns:
  - Current page: .../automotive-sales-in-australia-by-month
  - Recent years (2021+): .../automotive-sales-in-australia-by-month-{year}
  - Old format (2020-): .../salesfig_australia_{year}
"""

from __future__ import annotations

import logging
import random
import time

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from motor_vehicles.config import HttpConfig, MarklinesConfig

logger = logging.getLogger("motor_vehicles.scraping.marklines_client")


class MarklinesClient:
    """Fetches Marklines sales pages via httpx."""

    def __init__(self, http_config: HttpConfig, marklines_config: MarklinesConfig):
        self.http_config = http_config
        self.marklines_config = marklines_config
        self._client = httpx.Client(
            timeout=http_config.timeout_seconds,
            headers=http_config.default_headers,
            follow_redirects=True,
        )

    def close(self) -> None:
        self._client.close()

    def _delay(self) -> None:
        """Polite delay between requests."""
        delay = random.uniform(
            self.http_config.min_delay_seconds,
            self.http_config.max_delay_seconds,
        )
        time.sleep(delay)

    def _get_headers(self) -> dict[str, str]:
        """Headers with a random user agent."""
        ua = random.choice(self.http_config.user_agents)
        return {**self.http_config.default_headers, "User-Agent": ua}

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        reraise=True,
    )
    def _fetch(self, url: str) -> str:
        """Fetch a single URL and return the HTML content."""
        logger.info("Fetching %s", url)
        response = self._client.get(url, headers=self._get_headers())
        response.raise_for_status()
        return response.text

    def _build_url(self, year: int) -> str:
        """Build the URL for a given year using the appropriate format."""
        if year in self.marklines_config.recent_years:
            return f"{self.marklines_config.base_url}-{year}"
        return self.marklines_config.historical_url_template.format(year=year)

    def fetch_current_page(self) -> str:
        """Fetch the main Marklines page (most recent month)."""
        return self._fetch(self.marklines_config.base_url)

    def fetch_year_page(self, year: int) -> str:
        """Fetch a specific year page."""
        url = self._build_url(year)
        self._delay()
        return self._fetch(url)

    def fetch_all_pages(self) -> dict[str, str]:
        """Fetch all configured pages. Returns {url: html} dict."""
        pages: dict[str, str] = {}

        # Current page (most recent month)
        url = self.marklines_config.base_url
        try:
            pages[url] = self.fetch_current_page()
            logger.info("Fetched current page: %s", url)
        except httpx.HTTPStatusError as e:
            logger.error("Failed to fetch current page: %s", e)

        # Recent year pages (append -YYYY to base URL)
        for year in self.marklines_config.recent_years:
            year_url = self._build_url(year)
            if year_url in pages:
                continue
            try:
                pages[year_url] = self.fetch_year_page(year)
                logger.info("Fetched recent year page for %d", year)
            except httpx.HTTPStatusError as e:
                logger.warning("Failed to fetch %d page: %s", year, e)

        # Historical year pages (old URL format)
        for year in self.marklines_config.historical_years:
            hist_url = self._build_url(year)
            if hist_url in pages:
                continue
            try:
                pages[hist_url] = self.fetch_year_page(year)
                logger.info("Fetched historical page for %d", year)
            except httpx.HTTPStatusError as e:
                logger.warning("Failed to fetch %d page: %s", year, e)

        return pages
