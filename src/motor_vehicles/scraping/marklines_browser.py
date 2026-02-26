"""
Playwright-based fallback for Marklines when httpx gets blocked.
Renders the page with a headless browser to execute JavaScript.
"""

from __future__ import annotations

import logging

logger = logging.getLogger("motor_vehicles.scraping.marklines_browser")


class MarklinesBrowser:
    """Playwright-based Marklines page renderer."""

    def __init__(self, headless: bool = True):
        self.headless = headless
        self._browser = None
        self._playwright = None

    def start(self) -> None:
        """Launch the browser."""
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            raise RuntimeError(
                "Playwright is not installed. "
                "Install with: uv pip install 'motor-vehicles[browser]'"
            )

        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(headless=self.headless)
        logger.info("Playwright browser started (headless=%s)", self.headless)

    def close(self) -> None:
        """Close the browser and cleanup."""
        if self._browser:
            self._browser.close()
        if self._playwright:
            self._playwright.stop()
        logger.info("Playwright browser closed")

    def fetch_page(self, url: str, wait_ms: int = 3000) -> str:
        """Navigate to URL and return the rendered HTML."""
        assert self._browser is not None, "Browser not started. Call start() first."

        page = self._browser.new_page()
        try:
            logger.info("Navigating to %s", url)
            page.goto(url, wait_until="networkidle")
            page.wait_for_timeout(wait_ms)
            html = page.content()
            logger.info("Page rendered, %d bytes of HTML", len(html))
            return html
        finally:
            page.close()

    def fetch_all_pages(self, urls: list[str]) -> dict[str, str]:
        """Fetch multiple pages. Returns {url: html} dict."""
        pages: dict[str, str] = {}
        for url in urls:
            try:
                pages[url] = self.fetch_page(url)
            except Exception as e:
                logger.error("Browser failed to fetch %s: %s", url, e)
        return pages
