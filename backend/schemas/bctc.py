from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class BctcDocumentRow(BaseModel):
    doc_id: str
    ticker: str
    year: int | None
    period_key: str | None
    title: str | None
    normalized_title: str | None = None
    published_at: datetime | None
    doc_class: str | None
    canonical_priority: int | None = None
    is_consolidated: bool | None
    display_status: str | None
    is_available_for_web: bool | None
    url_pdf: str | None
    file_size: int | None
