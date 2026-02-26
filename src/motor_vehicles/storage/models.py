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
    source_url: str = ""


class MarklinesTotal(BaseModel):
    """Monthly total sales from Marklines chart data."""
    year: int
    month: int
    total_units: int | None = None
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
