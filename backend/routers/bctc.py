from __future__ import annotations

from datetime import date
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse, RedirectResponse
from psycopg2.extensions import connection as PgConn

from backend.config import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE
from backend.database import fetchall_as_dict, fetchone_as_dict
from backend.dependencies import get_db
from backend.schemas.common import PaginatedResponse
from backend.schemas.bctc import BctcDocumentRow

router = APIRouter(prefix="/bctc", tags=["bctc"])


@router.get("/documents", response_model=PaginatedResponse[BctcDocumentRow])
def list_all_bctc_documents(
    ticker: str | None = Query(None, description="Filter by ticker."),
    year: int | None = Query(None, description="Filter by fiscal/report year."),
    q: str | None = Query(None, description="Search in document title."),
    from_date: date | None = Query(None, alias="from"),
    to_date: date | None = Query(None, alias="to"),
    page: int = Query(1, ge=1),
    page_size: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    conn: PgConn = Depends(get_db),
):
    ticker_filter = ticker.upper() if ticker else None
    q_filter = f"%{q.strip()}%" if q and q.strip() else None
    offset = (page - 1) * page_size
    params = (
        ticker_filter,
        ticker_filter,
        year,
        year,
        q_filter,
        q_filter,
        q_filter,
        from_date,
        from_date,
        to_date,
        to_date,
    )

    where_clause = """
      is_available_for_web = true
      AND (%s IS NULL OR ticker = %s)
      AND (%s IS NULL OR year = %s)
      AND (%s IS NULL OR title ILIKE %s OR normalized_title ILIKE %s)
      AND (%s IS NULL OR published_at::date >= %s)
      AND (%s IS NULL OR published_at::date <= %s)
    """

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM gold.mart_bctc_documents
            WHERE {where_clause}
            """,
            params,
        )
        total = cur.fetchone()[0]

        cur.execute(
            f"""
            SELECT
                doc_id,
                ticker,
                year,
                period_key,
                title,
                normalized_title,
                published_at,
                doc_class,
                canonical_priority,
                is_consolidated,
                display_status,
                is_available_for_web,
                url_pdf,
                file_size
            FROM gold.mart_bctc_documents
            WHERE {where_clause}
            ORDER BY canonical_priority ASC NULLS LAST,
                     published_at DESC NULLS LAST,
                     year DESC NULLS LAST,
                     doc_id
            LIMIT %s OFFSET %s
            """,
            (*params, page_size, offset),
        )
        rows = fetchall_as_dict(cur)

    return PaginatedResponse(
        data=[BctcDocumentRow(**row) for row in rows],
        total=total,
        page=page,
        page_size=page_size,
        has_more=(offset + len(rows)) < total,
    )


@router.get("/recent", response_model=list[BctcDocumentRow])
def list_recent_bctc_documents(
    page_size: int = Query(10, ge=1, le=100),
    conn: PgConn = Depends(get_db),
):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                doc_id,
                ticker,
                year,
                period_key,
                title,
                normalized_title,
                published_at,
                doc_class,
                canonical_priority,
                is_consolidated,
                display_status,
                is_available_for_web,
                url_pdf,
                file_size
            FROM gold.mart_bctc_documents
            WHERE is_available_for_web = true
            ORDER BY canonical_priority ASC NULLS LAST,
                     published_at DESC NULLS LAST,
                     year DESC NULLS LAST,
                     doc_id
            LIMIT %s
            """,
            (page_size,),
        )
        rows = fetchall_as_dict(cur)

    return [BctcDocumentRow(**row) for row in rows]


@router.get("/{symbol}", response_model=list[BctcDocumentRow])
def list_bctc_documents(
    symbol: str,
    year: int | None = Query(None, description="Filter by fiscal/report year."),
    from_date: date | None = Query(None, alias="from"),
    to_date: date | None = Query(None, alias="to"),
    conn: PgConn = Depends(get_db),
):
    ticker = symbol.upper()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                doc_id,
                ticker,
                year,
                period_key,
                title,
                normalized_title,
                published_at,
                doc_class,
                canonical_priority,
                is_consolidated,
                display_status,
                is_available_for_web,
                url_pdf,
                file_size
            FROM gold.mart_bctc_documents
            WHERE ticker = %s
              AND is_available_for_web = true
              AND (%s IS NULL OR year = %s)
              AND (%s IS NULL OR published_at::date >= %s)
              AND (%s IS NULL OR published_at::date <= %s)
            ORDER BY year DESC NULLS LAST,
                     canonical_priority ASC NULLS LAST,
                     published_at DESC NULLS LAST
            """,
            (ticker, year, year, from_date, from_date, to_date, to_date),
        )
        rows = fetchall_as_dict(cur)

    if not rows:
        raise HTTPException(404, f"Khong tim thay tai lieu BCTC cho ticker: {symbol}")
    return [BctcDocumentRow(**row) for row in rows]


@router.get("/{symbol}/file/{doc_id}")
def get_bctc_file(
    symbol: str,
    doc_id: str,
    conn: PgConn = Depends(get_db),
):
    ticker = symbol.upper()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                doc_id,
                ticker,
                title,
                pdf_path,
                url_pdf,
                is_available_for_web
            FROM gold.mart_bctc_documents
            WHERE doc_id = %s
              AND ticker = %s
            """,
            (doc_id, ticker),
        )
        row = fetchone_as_dict(cur)

    if row is None:
        raise HTTPException(404, f"Khong tim thay tai lieu: {doc_id} / {symbol}")

    if not row.get("is_available_for_web"):
        raise HTTPException(403, "Tai lieu nay khong available de xem.")

    pdf_path = row.get("pdf_path")
    if pdf_path:
        path = Path(str(pdf_path)).expanduser()
        if not path.is_absolute():
            path = Path.cwd() / path
        if path.is_file():
            return FileResponse(
                path=str(path),
                media_type="application/pdf",
                filename=path.name,
                content_disposition_type="inline",
            )

    url_pdf = row.get("url_pdf")
    if url_pdf:
        return RedirectResponse(url=str(url_pdf), status_code=302)

    raise HTTPException(
        404,
        "File PDF khong tim thay tren server va khong co URL du phong.",
    )
