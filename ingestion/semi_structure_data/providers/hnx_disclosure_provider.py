from __future__ import annotations

import csv
import json
import logging
import os
import re
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from ingestion.common import call_with_retry, wait_for_rate_limit

from ..config import SemiStructuredIngestionConfig
from ..document_classifier import apply_canonical_selection, build_stable_doc_id, enrich_document_row

LOGGER = logging.getLogger(__name__)

_INCLUDE_BASE = (
    "bctc",
    "bao cao tai chinh",
    "bc tai chinh",
    "BaoCaoTaiChinh",
    "BaoCaoTaiChinhCtyMe",
    "BaoCaoTaiChinhCoBan",
    "BaoCaoTaiChinhCoBanCtyMe",
    "BaoCaoTaiChinhHopNhat"
)

# KHÔNG dùng annual_hint để loại cứng ở crawler nếu muốn giữ cả quý + năm
_INCLUDE_ANNUAL_HINT = (
    "nam",
    "kiem toan",
    "da kiem toan",
    "hop nhat",
)

_EXCLUDE_KEYWORDS_BCTC = (
    "uponrequest",
    "upon request",
    "tai lieu hop",
    "nghi quyet",
    "EN",
)

# Giữ trống hoặc chỉ loại các tài liệu ngoài phạm vi tài chính
_EXCLUDE_KEYWORDS = (
    "nghi quyet",
    "resolution",
    "EN"
)
_YEAR_PATTERN = re.compile(r"20\d{2}")

_HNX_ORIGIN = "https://www.hnx.vn"
_HNX_LIST_POST_URL = f"{_HNX_ORIGIN}/ModuleArticles/ArticlesCPEtfs/NextPageTinCPNY_CBTCPH"
_HNX_FILE_ATTACH_POST_URL = f"{_HNX_ORIGIN}/ModuleArticles/ArticlesCPEtfs/ArticlesFileAttach"

_API_UA_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36 stock-pipeline-bctc/1.0"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

_DATE_CELL_RE = re.compile(r"^\d{1,2}/\d{1,2}/\d{4}(?:\s+\d{1,2}:\d{2})?$")
_TICKER_CELL_RE = re.compile(r"^[A-Z0-9]{2,10}$")
# HNX đổi onclick theo thời kỳ: funcViewDetailArticlesByID / funcShowFileAttach; cho phép khoảng trắng, optional "return".
_ONCLICK_ARTICLE_RE = re.compile(
    r"(?:return\s+)?(?:func)?(?:ViewDetailArticlesByID|ShowFileAttach)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)",
    re.IGNORECASE,
)
_ONCLICK_ARTICLE_SINGLE_RE = re.compile(
    r"(?:return\s+)?(?:func)?(?:ViewDetailArticlesByID|ShowFileAttach)\s*\(\s*(\d+)\s*\)",
    re.IGNORECASE,
)
_HNX_FORM_CONTENT_TYPE = "application/x-www-form-urlencoded; charset=UTF-8"


def _max_list_pages(cfg: SemiStructuredIngestionConfig) -> int:
    return cfg.resolved_hnx_max_list_pages()


def _crawl_state_path(cfg: SemiStructuredIngestionConfig) -> Path:
    return cfg.semi_structure_root / "_state" / "hnx_crawl_state.json"


def _resume_from_state_enabled() -> bool:
    return os.environ.get("HNX_RESUME_FROM_STATE", "").strip().lower() in ("1", "true", "yes")


def _load_crawl_state(cfg: SemiStructuredIngestionConfig) -> dict[str, Any]:
    path = _crawl_state_path(cfg)
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as ex:
        LOGGER.warning("Khong doc duoc crawl state %s: %s", path, ex)
        return {}


def _save_crawl_state(cfg: SemiStructuredIngestionConfig, page: int) -> None:
    path = _crawl_state_path(cfg)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "last_success_page": int(page),
            "updated_at": __import__("datetime").datetime.utcnow().isoformat(),
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except OSError as ex:
        LOGGER.warning("Khong ghi duoc crawl state %s: %s", path, ex)


def _certifi_bundle() -> bool | str:
    try:
        import certifi

        return certifi.where()
    except ImportError:
        return True


def _hnx_requests_verify(cfg: SemiStructuredIngestionConfig) -> bool | str:
    """Thu tu uu tien: HNX_SSL_VERIFY env -> cfg.hnx_verify_ssl -> certifi."""
    env = os.environ.get("HNX_SSL_VERIFY", "").strip().lower()
    if env in ("0", "false", "no"):
        try:
            import urllib3

            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        except Exception:  # noqa: BLE001
            pass
        LOGGER.warning("HNX_SSL_VERIFY=0 — tat SSL verify cho www.hnx.vn")
        return False
    if env in ("1", "true", "yes"):
        return _certifi_bundle()
    if not cfg.hnx_verify_ssl:
        try:
            import urllib3

            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        except Exception:  # noqa: BLE001
            pass
        LOGGER.info(
            "hnx_verify_ssl=False (mac dinh dev) — tat SSL verify cho www.hnx.vn; "
            "dat hnx_verify_ssl=True hoac HNX_SSL_VERIFY=1 khi can an toan"
        )
        return False
    return _certifi_bundle()


def _session_headers() -> dict[str, str]:
    h = dict(_API_UA_HEADERS)
    h["X-Requested-With"] = "XMLHttpRequest"
    h["Referer"] = _HNX_ORIGIN
    h["Content-Type"] = _HNX_FORM_CONTENT_TYPE
    return h


def _extract_article_id_from_attrs(tag: Any) -> int | None:
    if not tag or not hasattr(tag, "attrs"):
        return None
    for key in (
        "data-articlesid",
        "data-articleid",
        "data-article-id",
        "data-article",
        "data-id",
        "data-article_id",
    ):
        raw = tag.attrs.get(key)
        if raw is None:
            continue
        raw_text = str(raw).strip()
        if raw_text.isdigit():
            return int(raw_text)
    return None


def _extract_article_id_from_onclick(onclick: str) -> tuple[int | None, int | None]:
    if not onclick:
        return None, None
    match = _ONCLICK_ARTICLE_RE.search(onclick)
    if match:
        return int(match.group(1)), int(match.group(2))
    match = _ONCLICK_ARTICLE_SINGLE_RE.search(onclick)
    if match:
        return int(match.group(1)), 0
    return None, None


def _parse_hnx_list_rows(html_fragment: str) -> list[dict[str, Any]]:
    """Parse HTML fragment list rows from HNX disclosure table."""
    soup = BeautifulSoup(html_fragment, "html.parser")
    rows: list[dict[str, Any]] = []
    for tr in soup.find_all("tr"):
        if tr.find_parent("thead"):
            continue

        published_at = ""
        ticker = ""
        title = ""
        article_id: int | None = None
        file_attach = 0
        title_link = tr.select_one("a.hrefViewDetail") or tr.select_one(
            'a[onclick*="ViewDetailArticlesByID"]'
        )
        if title_link:
            title = title_link.get_text(" ", strip=True)
            onclick = str(title_link.get("onclick") or "")
            article_id, file_attach = _extract_article_id_from_onclick(onclick)
            if article_id is None:
                article_id = _extract_article_id_from_attrs(title_link)

        attach_link = tr.select_one("a.icon-FileAttach") or tr.select_one(
            'a[onclick*="ShowFileAttach"]'
        )
        if attach_link:
            onclick = str(attach_link.get("onclick") or "")
            article_id_from_onclick, file_attach_from_onclick = _extract_article_id_from_onclick(onclick)
            if article_id_from_onclick is not None:
                article_id = article_id_from_onclick
                if file_attach_from_onclick is not None:
                    file_attach = file_attach_from_onclick
            if article_id is None:
                article_id = _extract_article_id_from_attrs(attach_link)

        symbol_link = tr.select_one("td.SYMBOL a") or tr.select_one("td a[href*='/chi-tiet-chung-khoan-']")
        symbol_guess = symbol_link.get_text(strip=True).upper() if symbol_link else ""

        for td in tr.find_all("td"):
            txt = td.get_text(strip=True)
            if not txt:
                continue
            if _DATE_CELL_RE.match(txt):
                published_at = txt
            if _TICKER_CELL_RE.match(txt):
                ticker = txt

        if not ticker and symbol_guess and _TICKER_CELL_RE.match(symbol_guess):
            ticker = symbol_guess

        if article_id is None:
            article_id = _extract_article_id_from_attrs(tr)

        if not title or article_id is None:
            continue

        rows.append(
            {
                "ticker": ticker,
                "title": title,
                "article_id": article_id,
                "file_attach": file_attach,
                "published_at": published_at,
            }
        )
    return rows


def _list_post_payload(page: int, cfg: SemiStructuredIngestionConfig) -> dict[str, str]:
    ma = ""
    if len(cfg.tickers) == 1:
        ma = str(cfg.tickers[0]).strip().upper()
    return {
        "pNumPage": str(page),
        "pAction": "0",
        "pNhomTin": "Báo cáo tài chính, Giải trình báo cáo tài chính",
        "pTieuDeTin": "báo cáo tài chính",
        "pMaChungKhoan": ma,
        "pFromDate": "",
        "pToDate": "",
        "pOrderBy": "",
    }


def _fetch_pdf_urls_from_detail(
    session: requests.Session,
    article_id: int,
    parent_title: str,
    ticker: str,
    cfg: SemiStructuredIngestionConfig,
) -> list[dict[str, str]]:
    def _do_post() -> str:
        wait_for_rate_limit(cfg.rate_limit_rpm)
        response = session.post(
            _HNX_FILE_ATTACH_POST_URL,
            data={"pArticlesID": str(article_id)},
            timeout=cfg.request_timeout_sec,
        )
        response.raise_for_status()
        return response.text

    try:
        html = call_with_retry(
            _do_post,
            max_attempts=cfg.api_retry_max_attempts,
            base_delay_sec=cfg.api_retry_base_delay_sec,
            label=f"hnx_file_attach:{article_id}",
        )
    except Exception as ex:  # noqa: BLE001
        LOGGER.warning("HNX file attach loi article_id=%s err=%s", article_id, ex)
        return []

    soup = BeautifulSoup(html, "html.parser")
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        raw_href = (a.get("href") or "").strip()
        if ".pdf" not in raw_href.lower():
            continue
        abs_url = urljoin(_HNX_ORIGIN + "/", raw_href)
        if abs_url in seen:
            continue
        seen.add(abs_url)

        attachment_name = a.get_text(" ", strip=True)
        effective_title = f"{parent_title} {attachment_name}".strip()
        if cfg.strict_bctc_annual_keyword_filter:
            is_match = _is_annual_bctc_candidate(
                attachment_name=attachment_name,
                title=effective_title,
                url_pdf=abs_url,
            )
        else:
            is_match = _is_bctc_candidate(
                attachment_name=attachment_name,
                title=effective_title,
                url_pdf=abs_url,
            )
        if not is_match:
            LOGGER.debug("HNX attachment skipped: %s", attachment_name)
            continue

        LOGGER.info("HNX attachment accepted: %s | %s", ticker, attachment_name)
        out.append({"attachment_name": attachment_name, "url_pdf": abs_url})
    return out


def _fetch_hnx_live_api_records(cfg: SemiStructuredIngestionConfig) -> list[dict[str, Any]]:
    """Crawl AJAX HTML HNX (POST phan trang), mo trang chi tiet lay link PDF dinh kem.

    Khong dung cfg.hnx_disclosure_api_url — endpoint co dinh tren www.hnx.vn.
    """
    session = requests.Session()
    session.headers.update(_session_headers())
    session.verify = _hnx_requests_verify(cfg)

    out: list[dict[str, Any]] = []
    detail_pdf_cache: dict[int, list[dict[str, str]]] = {}

    start_page = 1
    if _resume_from_state_enabled():
        state = _load_crawl_state(cfg)
        last_success = int(state.get("last_success_page") or 0)
        if last_success > 0:
            start_page = last_success + 1
            LOGGER.info(
                "HNX_RESUME_FROM_STATE=1 -> tiep tuc tu trang %s (last_success=%s)",
                start_page,
                last_success,
            )
    page = start_page

    max_pages = _max_list_pages(cfg)
    LOGGER.info(
        "HNX list crawl: max_pages=%s start_page=%s rate_limit_rpm=%s",
        max_pages,
        start_page,
        cfg.rate_limit_rpm,
    )
    while page <= max_pages:

        def _do_post() -> str:
            wait_for_rate_limit(cfg.rate_limit_rpm)
            response = session.post(
                _HNX_LIST_POST_URL,
                data=_list_post_payload(page, cfg),
                timeout=cfg.request_timeout_sec,
            )
            response.raise_for_status()
            return response.text

        try:
            fragment = call_with_retry(
                _do_post,
                max_attempts=cfg.api_retry_max_attempts,
                base_delay_sec=cfg.api_retry_base_delay_sec,
                label=f"hnx_list:p{page}",
            )
        except Exception as ex:  # noqa: BLE001
            LOGGER.warning("HNX list POST page=%s loi: %s", page, ex)
            break

        list_rows = _parse_hnx_list_rows(fragment)
        if not list_rows:
            if page == 1 and ("funcViewDetailArticlesByID" in fragment or "ViewDetailArticlesByID" in fragment):
                LOGGER.warning(
                    "HNX list trang 1: co onclick bai viet nhung parse 0 dong — co the doi HTML. "
                    "Snippet: %s",
                    fragment[:400].replace("\n", " "),
                )
            LOGGER.info("HNX list: het du lieu sau trang %s", page)
            break

        for row in list_rows:
            ticker = str(row.get("ticker") or "").strip().upper()
            title = str(row.get("title") or "").strip()
            published_raw = str(row.get("published_at") or "").strip()
            article_id = int(row.get("article_id"))
            if article_id not in detail_pdf_cache:
                detail_pdf_cache[article_id] = _fetch_pdf_urls_from_detail(
                    session,
                    article_id,
                    title,
                    ticker,
                    cfg,
                )
            attachments = detail_pdf_cache[article_id]
            if not attachments:
                continue
            year_guess = _extract_year(title, published_raw or None)
            for att in attachments:
                attachment_name = str(att.get("attachment_name") or "").strip()
                url_pdf = str(att.get("url_pdf") or "").strip()
                effective_title = f"{title} {attachment_name}".strip()
                out.append(
                    {
                        "ticker": ticker,
                        "title": effective_title,
                        "parent_title": title,
                        "attachment_name": attachment_name or None,
                        "published_at": published_raw or None,
                        "url_pdf": url_pdf,
                        "url_detail": f"{_HNX_ORIGIN}/vi-vn/thong-tin-cong-bo-ny-hnx.html",
                        "year": year_guess,
                    }
                )

        if _resume_from_state_enabled():
            _save_crawl_state(cfg, page)

        page += 1

    LOGGER.info("HNX AJAX crawl: %s ban ghi (truoc loc BCTC nam)", len(out))
    return out


def _normalize_text(value: str | None) -> str:
    text = (value or "").lower().strip()
    text = text.replace("á", "a").replace("à", "a").replace("ả", "a").replace("ã", "a").replace("ạ", "a")
    text = text.replace("ă", "a").replace("ắ", "a").replace("ằ", "a").replace("ẳ", "a").replace("ẵ", "a").replace("ặ", "a")
    text = text.replace("â", "a").replace("ấ", "a").replace("ầ", "a").replace("ẩ", "a").replace("ẫ", "a").replace("ậ", "a")
    text = text.replace("đ", "d")
    text = text.replace("é", "e").replace("è", "e").replace("ẻ", "e").replace("ẽ", "e").replace("ẹ", "e")
    text = text.replace("ê", "e").replace("ế", "e").replace("ề", "e").replace("ể", "e").replace("ễ", "e").replace("ệ", "e")
    text = text.replace("í", "i").replace("ì", "i").replace("ỉ", "i").replace("ĩ", "i").replace("ị", "i")
    text = text.replace("ó", "o").replace("ò", "o").replace("ỏ", "o").replace("õ", "o").replace("ọ", "o")
    text = text.replace("ô", "o").replace("ố", "o").replace("ồ", "o").replace("ổ", "o").replace("ỗ", "o").replace("ộ", "o")
    text = text.replace("ơ", "o").replace("ớ", "o").replace("ờ", "o").replace("ở", "o").replace("ỡ", "o").replace("ợ", "o")
    text = text.replace("ú", "u").replace("ù", "u").replace("ủ", "u").replace("ũ", "u").replace("ụ", "u")
    text = text.replace("ư", "u").replace("ứ", "u").replace("ừ", "u").replace("ử", "u").replace("ữ", "u").replace("ự", "u")
    text = text.replace("ý", "y").replace("ỳ", "y").replace("ỷ", "y").replace("ỹ", "y").replace("ỵ", "y")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def _extract_year(*values: str | None) -> str | None:
    for value in values:
        if not value:
            continue
        match = _YEAR_PATTERN.search(value)
        if match:
            return match.group(0)
    return None


def _build_haystack(attachment_name: str | None, title: str, url_pdf: str) -> str:
    return _normalize_text(f"{attachment_name or ''} {title} {url_pdf}")


def _passes_bctc_filters(haystack: str, *, strict_annual: bool) -> bool:
    if not any(keyword in haystack for keyword in _INCLUDE_BASE):
        return False
    if strict_annual and not any(keyword in haystack for keyword in _INCLUDE_ANNUAL_HINT):
        return False
    excludes = _EXCLUDE_KEYWORDS if strict_annual else _EXCLUDE_KEYWORDS_BCTC
    if any(keyword in haystack for keyword in excludes):
        return False
    return True


def _is_annual_bctc_candidate(
    attachment_name: str | None,
    title: str,
    url_pdf: str,
) -> bool:
    haystack = _build_haystack(attachment_name, title, url_pdf)
    return _passes_bctc_filters(haystack, strict_annual=True)


def _is_bctc_candidate(
    attachment_name: str | None,
    title: str,
    url_pdf: str,
) -> bool:
    haystack = _build_haystack(attachment_name, title, url_pdf)
    return _passes_bctc_filters(haystack, strict_annual=False)


def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _resolve_local_sample_path(cfg: SemiStructuredIngestionConfig) -> Path:
    if cfg.hnx_sample_json is not None:
        return cfg.hnx_sample_json
    return _project_root() / "data" / "hnx_sample.json"


def _resolve_urls_csv_path(cfg: SemiStructuredIngestionConfig) -> Path | None:
    if cfg.hnx_urls_csv is not None:
        return cfg.hnx_urls_csv if cfg.hnx_urls_csv.is_file() else None
    default = _project_root() / "data" / "hnx_urls.csv"
    return default if default.is_file() else None


def _row_get(row: dict[str, str], *keys: str) -> str:
    for k in keys:
        if k in row and str(row[k]).strip():
            return str(row[k]).strip()
        low = k.lower()
        for rk, rv in row.items():
            if str(rk).strip().lower() == low and str(rv).strip():
                return str(rv).strip()
    return ""


def _load_urls_csv_records(cfg: SemiStructuredIngestionConfig) -> list[dict[str, Any]]:
    path = _resolve_urls_csv_path(cfg)
    if path is None:
        return []
    out: list[dict[str, Any]] = []
    try:
        text = path.read_text(encoding="utf-8-sig")
    except OSError as ex:
        LOGGER.warning("Khong doc duoc CSV HNX (%s): %s", path, ex)
        return []
    reader = csv.DictReader(text.splitlines())
    if not reader.fieldnames:
        return []
    for raw in reader:
        row = {str(k or "").strip(): (raw[k] or "").strip() if k else "" for k in raw}
        url_pdf = _row_get(row, "url_pdf", "pdf_url", "url", "link")
        if not url_pdf:
            continue
        ticker = _row_get(row, "ticker", "symbol", "ma")
        title = _row_get(row, "title", "tieude", "ten")
        published_at = _row_get(row, "published_at", "ngay", "date") or None
        url_detail = _row_get(row, "url_detail", "detail") or None
        year = _row_get(row, "year", "nam") or None
        if not title:
            y = year or _extract_year(url_pdf, ticker) or "20XX"
            t = ticker or "UNKNOWN"
            title = f"Báo cáo tài chính năm {y} đã kiểm toán — {t}"
        out.append(
            {
                "ticker": ticker,
                "title": title,
                "published_at": published_at,
                "url_pdf": url_pdf,
                "url_detail": url_detail,
                "year": year or None,
            }
        )
    LOGGER.info("Doc %s dong tu CSV %s", len(out), path)
    return out


def _load_local_sample_records(cfg: SemiStructuredIngestionConfig) -> list[dict[str, Any]]:
    sample_path = _resolve_local_sample_path(cfg)
    if not sample_path.is_file():
        LOGGER.warning("Khong tim thay sample HNX local tai %s", sample_path)
        return []
    try:
        payload = json.loads(sample_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as ex:
        LOGGER.warning("Khong doc duoc JSON sample HNX (%s): %s", sample_path, ex)
        return []
    if isinstance(payload, dict):
        rows = payload.get("records") or payload.get("items") or []
        if isinstance(rows, list):
            return [x for x in rows if isinstance(x, dict)]
        return []
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    return []


def fetch_hnx_annual_bctc_documents(cfg: SemiStructuredIngestionConfig) -> list[dict[str, Any]]:
    """Lay danh sach tai lieu BCTC nam HNX.

    Nguon (gop theo thu tu, loai trung url_pdf):
    - **Crawl chinh thuc HNX:** POST AJAX + HTML (`_fetch_hnx_live_api_records`, khong can URL tu config).
    - `data/hnx_sample.json` (hoac cfg.hnx_sample_json)
    - `data/hnx_urls.csv` (hoac cfg.hnx_urls_csv)
    """
    records: list[dict[str, Any]] = []
    records.extend(_fetch_hnx_live_api_records(cfg))
    records.extend(_load_local_sample_records(cfg))
    records.extend(_load_urls_csv_records(cfg))
    seen_url: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for row in records:
        u = str(row.get("url_pdf") or row.get("pdf_url") or "").strip()
        if not u or u in seen_url:
            continue
        seen_url.add(u)
        deduped.append(row)
    records = deduped

    out: list[dict[str, Any]] = []
    ticker_filter = {x.upper() for x in cfg.tickers}
    for row in records:
        title = str(row.get("title") or "").strip()
        url_pdf = str(row.get("url_pdf") or row.get("pdf_url") or "").strip()
        if not url_pdf:
            continue
        if not title:
            tkr = str(row.get("ticker") or "").strip().upper() or "UNKNOWN"
            title = f"Công bố HNX — {tkr}"
        if cfg.strict_bctc_annual_keyword_filter and not _is_annual_bctc_candidate(
            row.get("attachment_name"),
            title,
            url_pdf,
        ):
            continue
        ticker = str(row.get("ticker") or "").strip().upper()
        if ticker_filter and ticker and ticker not in ticker_filter:
            continue
        year = row.get("year")
        year_text = str(year).strip() if year is not None else _extract_year(title, url_pdf)
        normalized = {
            "source": "hnx",
            "ticker": ticker,
            "title": title,
            "published_at": row.get("published_at"),
            "url_pdf": url_pdf,
            "url_detail": row.get("url_detail"),
            "year": year_text or None,
            "attachment_name": row.get("attachment_name"),
        }
        out.append(normalized)
    enriched: list[dict[str, Any]] = []
    for row in out:
        enriched.append(enrich_document_row(dict(row), cfg))
    apply_canonical_selection(enriched, cfg)
    for d in enriched:
        did = build_stable_doc_id(d)
        LOGGER.info(
            "doc_classify | doc_id=%s ticker=%s normalized_title=%s doc_class=%s language=%s "
            "keep_for_parse=%s canonical_priority=%s",
            did,
            str(d.get("ticker") or ""),
            str(d.get("normalized_title") or "")[:240],
            d.get("doc_class"),
            d.get("language"),
            d.get("keep_for_parse"),
            d.get("canonical_priority"),
        )
    LOGGER.info("HNX annual BCTC documents sau loc + classify: %s", len(enriched))
    return enriched

