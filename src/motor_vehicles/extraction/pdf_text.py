"""
Text extraction from FCAI PDF publications.
Provides raw text extraction as a fallback when table extraction fails.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pdfplumber

logger = logging.getLogger("motor_vehicles.extraction.pdf_text")


def extract_text(filepath: str | Path) -> str:
    """Extract all text from a PDF file."""
    filepath = Path(filepath)
    if not filepath.exists():
        logger.warning("PDF not found: %s", filepath)
        return ""

    pages_text: list[str] = []
    try:
        with pdfplumber.open(filepath) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages_text.append(text)
    except Exception as e:
        logger.error("Failed to extract text from %s: %s", filepath.name, e)
        return ""

    full_text = "\n\n".join(pages_text)
    logger.info("Extracted %d characters from %s", len(full_text), filepath.name)
    return full_text


def extract_text_by_page(filepath: str | Path) -> list[str]:
    """Extract text from each page of a PDF. Returns list of page texts."""
    filepath = Path(filepath)
    if not filepath.exists():
        return []

    pages: list[str] = []
    try:
        with pdfplumber.open(filepath) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""
                pages.append(text)
    except Exception as e:
        logger.error("Failed to extract text from %s: %s", filepath.name, e)
        return []

    return pages
