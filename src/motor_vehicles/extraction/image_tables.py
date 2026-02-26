"""
Image table extraction using Vision LLM (OpenRouter or Anthropic API).

Downloads article images and extracts structured table data
by sending images to a vision-capable model via the configured provider.
"""

from __future__ import annotations

import base64
import json
import logging
from pathlib import Path

import httpx

from motor_vehicles.config import VisionConfig

logger = logging.getLogger("motor_vehicles.extraction.image_tables")

TABLE_EXTRACTION_PROMPT = """You are a precise data extraction tool. Extract ALL tables from this image.

CRITICAL RULES:
1. Extract EVERY table separately. Side-by-side tables are separate entries.
2. Include ALL rows -- do NOT truncate or summarize.
3. Multi-line column headers should be joined into a single string per column.
4. Each row must have the same number of values as there are headers.
5. Preserve exact cell values as strings (e.g., "1,234", "5.6%", "-18.3%").
6. Use empty strings for blank cells, never null.

Return a JSON object:
{
  "tables": [
    {
      "title": "table title if visible (or empty string)",
      "headers": ["Column1", "Column2", ...],
      "rows": [
        ["value1", "value2", ...],
        ["value3", "value4", ...]
      ]
    }
  ]
}

Return ONLY the JSON object, no markdown fences or explanation."""

MEDIA_TYPES = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".gif": "image/gif",
    ".webp": "image/webp",
}


def download_article_image(
    client: httpx.Client,
    url: str,
    download_dir: str | Path,
) -> Path:
    """Download an image from a URL. Skips if file already exists."""
    download_dir = Path(download_dir)
    download_dir.mkdir(parents=True, exist_ok=True)

    filename = url.split("/")[-1].split("?")[0]
    filepath = download_dir / filename

    if filepath.exists():
        logger.info("Image already exists: %s", filepath)
        return filepath

    logger.info("Downloading image: %s", url)
    response = client.get(url)
    response.raise_for_status()
    filepath.write_bytes(response.content)
    logger.info("Saved image: %s (%d bytes)", filepath, len(response.content))

    return filepath


def extract_tables_from_image(
    image_path: str | Path,
    vision_config: VisionConfig | None = None,
) -> list[dict]:
    """Extract table data from an image using a Vision LLM.

    Uses VisionConfig to determine provider (openrouter or anthropic).
    Requires OPENROUTER_API_KEY or ANTHROPIC_API_KEY environment variable.

    Returns a list of dicts, each with:
        - headers: list[str]
        - rows: list[list[str]]
        - dataframe_csv: str (CSV representation)
        - table_index: int
        - confidence: float
    """
    if vision_config is None:
        vision_config = VisionConfig()

    image_path = Path(image_path)
    logger.info("Extracting tables from image: %s", image_path.name)

    if not vision_config.api_key:
        logger.error(
            "No API key set for provider %s, cannot extract tables",
            vision_config.provider,
        )
        return []

    # Encode image
    image_data = base64.standard_b64encode(image_path.read_bytes()).decode("utf-8")
    suffix = image_path.suffix.lower()
    media_type = MEDIA_TYPES.get(suffix, "image/png")

    # Call vision API
    if vision_config.provider == "openrouter":
        response_text = _call_openrouter(vision_config, image_data, media_type)
    else:
        response_text = _call_anthropic(vision_config, image_data, media_type)

    if not response_text:
        logger.warning("Empty response from Vision LLM for %s", image_path.name)
        return []

    # Parse JSON response
    tables = _parse_response(response_text, image_path.name)
    logger.info("Extracted %d table(s) from %s", len(tables), image_path.name)
    return tables


def _call_openrouter(
    config: VisionConfig,
    image_b64: str,
    media_type: str,
) -> str:
    """Call OpenRouter's OpenAI-compatible endpoint via httpx."""
    url = config.api_base_url or "https://openrouter.ai/api/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {config.api_key}",
        "Content-Type": "application/json",
    }
    body = {
        "model": config.model,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": TABLE_EXTRACTION_PROMPT},
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:{media_type};base64,{image_b64}",
                        },
                    },
                ],
            }
        ],
        "max_tokens": config.max_tokens,
    }

    resp = httpx.post(
        url,
        headers=headers,
        json=body,
        timeout=config.timeout_seconds,
    )
    resp.raise_for_status()
    data = resp.json()
    return data["choices"][0]["message"]["content"]


def _call_anthropic(
    config: VisionConfig,
    image_b64: str,
    media_type: str,
) -> str:
    """Call Anthropic's API directly."""
    import anthropic

    client = anthropic.Anthropic(api_key=config.api_key)
    message = client.messages.create(
        model=config.model,
        max_tokens=config.max_tokens,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": media_type,
                            "data": image_b64,
                        },
                    },
                    {
                        "type": "text",
                        "text": TABLE_EXTRACTION_PROMPT,
                    },
                ],
            },
        ],
    )
    return message.content[0].text if message.content else ""


def _parse_response(response_text: str, source_name: str) -> list[dict]:
    """Parse the Vision LLM JSON response into structured table dicts."""
    # Strip markdown fences if present
    text = response_text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        # Remove first and last lines (fences)
        lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines)

    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        logger.error("Failed to parse JSON response for %s: %s", source_name, e)
        logger.debug("Response text: %s", text[:500])
        return []

    raw_tables = data.get("tables", [])
    results = []

    for idx, table in enumerate(raw_tables):
        headers = table.get("headers", [])
        rows = table.get("rows", [])

        # Build CSV representation
        import csv
        import io
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(headers)
        writer.writerows(rows)
        dataframe_csv = buf.getvalue()

        results.append({
            "headers": headers,
            "rows": rows,
            "dataframe_csv": dataframe_csv,
            "table_index": idx,
            "confidence": 0.85,
        })

    return results
