"""
FCAI article scraper for media release pages.

Fetches article web pages from fcai.com.au, extracts metadata,
body text, and embedded image URLs for Vision LLM table extraction.
"""

from __future__ import annotations

import logging
import random
import re
import time
from dataclasses import dataclass, field
from datetime import date
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

from motor_vehicles.config import FcaiArticlesConfig, HttpConfig

logger = logging.getLogger("motor_vehicles.scraping.fcai_articles")

MONTH_NAMES = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

SALES_KEYWORDS = [
    "vehicle sales", "new car sales", "new vehicle",
    "vfacts", "sales results", "sales figures",
    "sales data", "market update", "automotive sales",
    "market remains", "market holds", "sales remain",
    "sales steady", "sales records", "buyer confidence",
    "hybrids build momentum", "hybrids gain momentum",
    "plug-in hybrids", "sales in may", "sales in june",
    "sales in july", "sales in august", "sales in september",
    "sales in october", "sales in november", "sales in december",
    "sales in january", "sales in february", "sales in march",
    "sales in april", "utes dominate", "market slows",
    "consumer shift to evs", "consumer demand",
    "slow start for new", "solid vehicle",
    "record but outlook", "sales reflect",
    "sales reach one million",
]

# Titles containing these indicate non-vehicle-sales articles
EXCLUDE_KEYWORDS = [
    "motorcycle", "scooter", "atv", "side x side",
    "side-by-side", "off-road alert", "road safety",
    "tyre stewardship", "recycling", "end-of-life",
    "nves report", "climate target", "ev charger",
    "budget", "membership", "board", "court",
    "hub for atv", "easter", "emissions",
    "infrastructure", "dealer case",
]


@dataclass
class ArticleListing:
    """Summary from the media releases listing page."""
    url: str
    title: str
    date_text: str = ""


@dataclass
class ArticleDetail:
    """Fully parsed article page."""
    url: str
    slug: str
    title: str
    published_date: date | None = None
    year: int | None = None
    month: int | None = None
    body_text: str = ""
    image_urls: list[str] = field(default_factory=list)
    image_labels: list[str] = field(default_factory=list)
    html_tables: list[dict] = field(default_factory=list)
    is_sales_article: bool = False


class FcaiArticleScraper:
    """Fetches and parses FCAI media release articles."""

    def __init__(self, http_config: HttpConfig, articles_config: FcaiArticlesConfig):
        self.http_config = http_config
        self.articles_config = articles_config
        self._client = httpx.Client(
            timeout=http_config.timeout_seconds,
            headers=http_config.default_headers,
            follow_redirects=True,
        )

    def close(self) -> None:
        self._client.close()

    def _delay(self) -> None:
        delay = random.uniform(
            self.http_config.min_delay_seconds,
            self.http_config.max_delay_seconds,
        )
        time.sleep(delay)

    def _get_headers(self) -> dict[str, str]:
        ua = random.choice(self.http_config.user_agents)
        return {**self.http_config.default_headers, "User-Agent": ua}

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        reraise=True,
    )
    def _fetch(self, url: str) -> str:
        logger.info("Fetching %s", url)
        response = self._client.get(url, headers=self._get_headers())
        response.raise_for_status()
        return response.text

    def fetch_article(self, url: str) -> ArticleDetail:
        """Fetch and parse a single article page."""
        html = self._fetch(url)
        soup = BeautifulSoup(html, "lxml")

        # Slug from URL
        slug = url.rstrip("/").split("/")[-1]

        # Title - prefer entry-title class, then h2, then h1, then <title>
        title = ""
        entry_title = soup.find(class_="entry-title")
        if entry_title:
            title = entry_title.get_text(strip=True)
        else:
            h2 = soup.find("h2")
            if h2:
                title = h2.get_text(strip=True)
            else:
                h1 = soup.find("h1")
                if h1:
                    title = h1.get_text(strip=True)
                elif soup.title:
                    title = soup.title.get_text(strip=True)

        # Published date
        published_date = _extract_published_date(soup)

        # Body content
        body_el = soup.find("article") or soup.find(class_="entry-content")
        body_text = ""
        image_urls: list[str] = []
        image_labels: list[str] = []

        if body_el:
            body_text = body_el.get_text(separator="\n", strip=True)

            # Extract images with wp-content/uploads in src (filters logos/icons)
            for img in body_el.find_all("img"):
                src = img.get("src", "")
                if "wp-content/uploads" in src:
                    # Sanitize Unicode whitespace in URLs (e.g. narrow no-break space)
                    src = _sanitize_url(src)
                    full_url = urljoin(url, src)
                    if full_url not in image_urls:
                        image_urls.append(full_url)
                        label = _find_image_label(img)
                        image_labels.append(label)

        # Extract HTML tables from body
        html_tables: list[dict] = []
        if body_el:
            for table_el in body_el.find_all("table"):
                html_tables.append(_parse_html_table(table_el))

        # Infer year/month from title and published_date
        year, month = _infer_year_month(title, published_date)

        # Classify as sales article
        is_sales = classify_sales_article(title)

        return ArticleDetail(
            url=url,
            slug=slug,
            title=title,
            published_date=published_date,
            year=year,
            month=month,
            body_text=body_text,
            image_urls=image_urls,
            image_labels=image_labels,
            html_tables=html_tables,
            is_sales_article=is_sales,
        )

    def fetch_article_listings(self, max_pages: int | None = None) -> list[ArticleListing]:
        """Fetch media release listing pages.

        Parses the search-filter-results container where each article is
        a div containing an h3 > a link followed by a p with the date.
        """
        if max_pages is None:
            max_pages = self.articles_config.max_pages

        listings: list[ArticleListing] = []
        base = self.articles_config.listing_url

        for page_num in range(1, max_pages + 1):
            params = dict(self.articles_config.listing_params)
            if page_num > 1:
                params["sf_paged"] = str(page_num)

            try:
                url = base
                if params:
                    qs = "&".join(f"{k}={v}" for k, v in params.items())
                    url = f"{base}?{qs}"

                html = self._fetch(url)
                soup = BeautifulSoup(html, "lxml")

                # Articles are h3 > a inside div.search-filter-results
                container = soup.find("div", class_="search-filter-results")
                if not container:
                    logger.info("No results container on page %d, stopping", page_num)
                    break

                headings = container.find_all("h3")
                if not headings:
                    logger.info("No article headings on page %d, stopping", page_num)
                    break

                page_count = 0
                for h3 in headings:
                    link = h3.find("a", href=True)
                    if not link:
                        continue
                    art_url = urljoin(base, link["href"])
                    art_title = link.get_text(strip=True)

                    # Date is in the next <p> sibling after the h3
                    date_text = ""
                    for sib in h3.next_siblings:
                        if hasattr(sib, "name") and sib.name == "p":
                            text = sib.get_text(strip=True)
                            if text:
                                date_text = text
                            break

                    listings.append(ArticleListing(
                        url=art_url,
                        title=art_title,
                        date_text=date_text,
                    ))
                    page_count += 1

                logger.info("Page %d: found %d articles", page_num, page_count)
                if page_num < max_pages:
                    self._delay()

            except Exception as e:
                logger.warning("Failed to fetch listing page %d: %s", page_num, e)
                break

        return listings


def _sanitize_url(url: str) -> str:
    """Replace Unicode whitespace characters with URL-encoded equivalents."""
    # Common offenders: \u202f (narrow no-break space), \u00a0 (no-break space)
    url = url.replace("\u202f", "%E2%80%AF")
    url = url.replace("\u00a0", "%C2%A0")
    url = url.replace(" ", "%20")
    return url


def classify_sales_article(title: str) -> bool:
    """Check if article title indicates a monthly vehicle sales data release.

    Returns True for articles reporting on monthly new vehicle sales.
    Returns False for motorcycle reports, policy/safety articles, etc.
    """
    title_lower = title.lower()

    # Exclude non-vehicle-sales articles first
    if any(kw in title_lower for kw in EXCLUDE_KEYWORDS):
        return False

    return any(kw in title_lower for kw in SALES_KEYWORDS)


def _extract_published_date(soup: BeautifulSoup) -> date | None:
    """Extract published date from meta tags or <time> element."""
    # Try meta tag first
    meta = soup.find("meta", property="article:published_time")
    if meta and meta.get("content"):
        try:
            return date.fromisoformat(meta["content"][:10])
        except ValueError:
            pass

    # Try <time> element
    time_el = soup.find("time", datetime=True)
    if time_el:
        try:
            return date.fromisoformat(time_el["datetime"][:10])
        except ValueError:
            pass

    return None


def _find_image_label(img_tag) -> str:
    """Find the label/heading text immediately before an image."""
    # Walk backwards through previous siblings
    for sibling in img_tag.parent.previous_siblings if img_tag.parent else []:
        if hasattr(sibling, "name"):
            if sibling.name in ("strong", "b", "h2", "h3", "h4", "h5", "h6"):
                text = sibling.get_text(strip=True)
                if text:
                    return text
            # Check for <p> containing <strong>/<b>
            if sibling.name == "p":
                bold = sibling.find(["strong", "b"])
                if bold:
                    text = bold.get_text(strip=True)
                    if text:
                        return text
            # Stop if we hit a block element with substantial text
            if sibling.name in ("p", "div", "table") and len(sibling.get_text(strip=True)) > 50:
                break

    # Also check the parent's previous siblings (image might be in a <p> or <figure>)
    parent = img_tag.parent
    if parent and parent.parent:
        for sibling in parent.previous_siblings:
            if hasattr(sibling, "name"):
                if sibling.name in ("strong", "b", "h2", "h3", "h4", "h5", "h6"):
                    text = sibling.get_text(strip=True)
                    if text:
                        return text
                if sibling.name == "p":
                    bold = sibling.find(["strong", "b"])
                    if bold:
                        text = bold.get_text(strip=True)
                        if text:
                            return text
                if sibling.name in ("p", "div", "table") and len(sibling.get_text(strip=True)) > 50:
                    break

    return ""


def _parse_html_table(table_el) -> dict:
    """Extract headers and row data from an HTML <table> element.

    Returns a dict with:
        headers: list[str]
        rows: list[list[str]]
    """
    headers: list[str] = []
    rows: list[list[str]] = []

    # Extract headers from <thead> or first <tr> with <th>
    thead = table_el.find("thead")
    if thead:
        for th in thead.find_all("th"):
            headers.append(th.get_text(strip=True))
    else:
        first_row = table_el.find("tr")
        if first_row:
            ths = first_row.find_all("th")
            if ths:
                headers = [th.get_text(strip=True) for th in ths]

    # Extract data rows from <tbody> or all <tr> elements
    tbody = table_el.find("tbody") or table_el
    for tr in tbody.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if not cells:
            continue
        row = [cell.get_text(strip=True) for cell in cells]
        # Skip rows that are all headers (already captured)
        if row == headers:
            continue
        rows.append(row)

    return {"headers": headers, "rows": rows}


def _infer_year_month(title: str, published_date: date | None) -> tuple[int | None, int | None]:
    """Infer the data year and month from the article title and/or published date.

    FCAI publishes monthly sales reports in the first week of the following
    month. So an article published 2025-02-05 reports on January 2025 data.
    We first try to extract an explicit month name from the title, then fall
    back to published_date.month - 1.
    """
    title_lower = title.lower()

    # Try to find month name in title
    month_num = None
    for name, num in MONTH_NAMES.items():
        if name in title_lower:
            month_num = num
            break

    # Try to find year in title
    year_match = re.search(r"\b(20\d{2})\b", title)
    year_num = int(year_match.group(1)) if year_match else None

    # Fall back to published_date
    if published_date:
        if year_num is None:
            year_num = published_date.year

        if month_num is None:
            # Data month is the month before publication
            if published_date.month == 1:
                month_num = 12
                # Also adjust year if not explicitly in title
                if year_num == published_date.year:
                    year_num = published_date.year - 1
            else:
                month_num = published_date.month - 1

    return year_num, month_num
