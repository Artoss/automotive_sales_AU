"""
HTTP client for downloading FCAI PDF publications.
Uses httpx with tenacity retry/backoff.
"""

from __future__ import annotations

import hashlib
import logging
import random
import time
from pathlib import Path

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from motor_vehicles.config import FcaiConfig, HttpConfig

logger = logging.getLogger("motor_vehicles.scraping.fcai_client")


class FcaiClient:
    """Downloads FCAI PDF publications."""

    def __init__(self, http_config: HttpConfig, fcai_config: FcaiConfig):
        self.http_config = http_config
        self.fcai_config = fcai_config
        self.download_dir = Path(fcai_config.download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
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

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        reraise=True,
    )
    def _download(self, url: str, filepath: Path) -> int:
        """Download a file and return its size in bytes."""
        ua = random.choice(self.http_config.user_agents)
        headers = {**self.http_config.default_headers, "User-Agent": ua}

        logger.info("Downloading %s", url)
        with self._client.stream("GET", url, headers=headers) as response:
            response.raise_for_status()
            with open(filepath, "wb") as f:
                for chunk in response.iter_bytes(chunk_size=8192):
                    f.write(chunk)

        return filepath.stat().st_size

    def download_pdf(self, entry: dict) -> dict:
        """Download a single PDF. Returns result dict with filepath, hash, size, skipped."""
        filepath = self.download_dir / entry["filename"]

        # Skip if already downloaded
        if filepath.exists():
            file_hash = _compute_hash(filepath)
            return {
                "filepath": str(filepath),
                "file_hash": file_hash,
                "file_size_bytes": filepath.stat().st_size,
                "skipped": True,
            }

        self._delay()
        size = self._download(entry["url"], filepath)
        file_hash = _compute_hash(filepath)

        logger.info("Downloaded %s (%d bytes, hash=%s)", entry["filename"], size, file_hash[:12])
        return {
            "filepath": str(filepath),
            "file_hash": file_hash,
            "file_size_bytes": size,
            "skipped": False,
        }


def _compute_hash(filepath: Path) -> str:
    """Compute SHA256 hash of a file."""
    sha256 = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()
