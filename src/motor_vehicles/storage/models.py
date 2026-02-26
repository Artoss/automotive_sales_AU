"""
Pydantic models for database rows.
"""

from __future__ import annotations

from pydantic import BaseModel


class MarklinesSale(BaseModel):
    """Monthly sales record by make from Marklines."""
    year: int
    month: int
    make: str
    units_sold: int | None = None
    market_share: float | None = None
    units_sold_prev_year: int | None = None
    yoy_pct: float | None = None
    source_url: str = ""


class MarklinesVehicleTypeSale(BaseModel):
    """Monthly vehicle type breakdown from Marklines."""
    year: int
    month: int
    vehicle_type: str
    units_sold: int | None = None
    units_sold_prev_year: int | None = None
    yoy_pct: float | None = None
    source_url: str = ""


class MarklinesCommentary(BaseModel):
    """Monthly text commentary from Marklines flash reports."""
    year: int
    month: int
    report_date: str = ""
    commentary: str
    source_url: str = ""


class FcaiPublication(BaseModel):
    """Tracking record for an FCAI PDF publication."""
    year: int
    month: int
    filename: str
    url: str
    file_hash: str = ""
    file_size_bytes: int = 0


class FcaiSalesRecord(BaseModel):
    """Sales data extracted from an FCAI PDF."""
    year: int
    month: int
    make: str = ""
    model: str = ""
    segment: str = ""
    fuel_type: str = ""
    units_sold: int | None = None
    market_share: float | None = None


class FcaiArticle(BaseModel):
    """An FCAI media release article."""
    url: str
    slug: str
    title: str
    published_date: str | None = None
    year: int | None = None
    month: int | None = None
    article_text: str = ""
    is_sales_article: bool = False


class FcaiArticleImage(BaseModel):
    """An image embedded in an FCAI article."""
    image_url: str
    image_filename: str
    local_path: str = ""
    image_order: int = 0
    image_label: str = ""
    width: int | None = None
    height: int | None = None


class FcaiArticleExtractedTable(BaseModel):
    """Table data extracted from an article image via Vision LLM."""
    table_index: int = 0
    headers: list = []
    row_data: list = []
    dataframe_csv: str = ""
    extraction_method: str = "vision_llm"
    confidence: float = 0.85
