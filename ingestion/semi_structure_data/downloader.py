"""Robust PDF downloader cho BCTC ingestion.

Thiết kế:
- Stream-to-disk: ghi từng chunk vào `<target>.pdf.part`, KHÔNG buffer cả file vào RAM.
- Atomic rename: chỉ `os.replace(part, final)` khi pass đủ integrity checks.
- Resume (Phase 1.5): nếu `.part` đã tồn tại với size>0, gửi `Range: bytes=<size>-`.
    * 206 -> append tiếp vào .part.
    * 200 -> server không hỗ trợ range, xoá .part và restart full download.
    * 416 -> .part stale/sai range, xoá .part và retry.
- Retry policy tự quản (không phụ thuộc urllib3 Retry để tránh "double retry"):
    * Retry transient network errors: ConnectionError, ReadTimeout, ChunkedEncodingError,
      ProtocolError, IncompleteRead, OSError/TimeoutError.
    * Retry HTTP: 408, 425, 429, 500, 502, 503, 504 (tôn trọng Retry-After nếu là số).
    * Fail-fast HTTP: 400, 401, 403, 404, 405, 410.
    * Exponential backoff + jitter, có cap.
- Integrity checks trên file `.part`:
    * Header bắt đầu bằng `%PDF-`.
    * Nếu response có Content-Length: bytes thực tế == total expected.
    * Best-effort tail check tìm `%%EOF` trong cửa sổ 8KB cuối.
      Nếu thiếu `%%EOF` mà còn retry budget -> coi là suspicious, xoá .part và retry.
- Khi fail cuối cùng:
    * Lỗi mạng/interrupted -> GIỮ `.part` để resume lần sau.
    * Lỗi non-PDF content / HTML / fail-fast HTTP -> xoá `.part`.
    * Lỗi integrity (corrupt) -> xoá `.part` trong vòng retry; sau cùng có thể đã không còn.
- Logging structured per-attempt: doc_id, host, attempt, resumed, status_code,
  bytes_downloaded, content_length, elapsed_seconds, error_class, retrying.

KHÔNG thay đổi public API của package.
"""

from __future__ import annotations

import logging
import os
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

import requests

from .common import ensure_dir, is_pdf_file_valid
from .config import SemiStructuredIngestionConfig
from .http_client import host_of, timeout_for_url, verify_for_url

LOGGER = logging.getLogger(__name__)

_CHUNK_SIZE = 256 * 1024
_EOF_TAIL_WINDOW = 8192
_BACKOFF_BASE = 1.5
_BACKOFF_CAP = 30.0

_RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}
_FAIL_FAST_STATUS = {400, 401, 403, 404, 405, 410}

_NON_PDF_CONTENT_HINTS = ("text/html", "application/xhtml", "text/plain", "application/json")


@dataclass
class DownloadResult:
    """Kết quả 1 lượt tải. Map 1-1 sang các field metadata phía ingestor."""

    success: bool = False
    http_status: Optional[int] = None
    content_length: Optional[int] = None
    bytes_downloaded: int = 0
    download_seconds: float = 0.0
    attempts: int = 0
    integrity_ok: bool = False
    error_class: Optional[str] = None
    error: Optional[str] = None
    resumed: bool = False
    final_path: Optional[Path] = None


def _classify_exception(exc: BaseException) -> tuple[bool, str]:
    """Trả về (retryable, error_class_name)."""
    name = type(exc).__name__
    if isinstance(
        exc,
        (
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.ConnectionError,
            requests.exceptions.ReadTimeout,
            requests.exceptions.ConnectTimeout,
            requests.exceptions.Timeout,
        ),
    ):
        return True, name
    try:
        import urllib3.exceptions as u3

        if isinstance(
            exc,
            (u3.ProtocolError, u3.IncompleteRead, u3.ReadTimeoutError, u3.NewConnectionError),
        ):
            return True, name
    except Exception:  # noqa: BLE001
        pass
    try:
        import http.client as hc

        if isinstance(exc, hc.IncompleteRead):
            return True, name
    except Exception:  # noqa: BLE001
        pass
    if isinstance(exc, (TimeoutError, OSError)):
        return True, name
    return False, name


def _sleep_backoff(attempt: int) -> float:
    delay = min(_BACKOFF_CAP, _BACKOFF_BASE * (2 ** (attempt - 1)))
    delay += random.uniform(0, 0.5)
    time.sleep(delay)
    return delay


def _parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    v = value.strip()
    if v.isdigit():
        try:
            return float(v)
        except ValueError:
            return None
    return None


def _has_eof_marker(path: Path) -> bool:
    try:
        size = path.stat().st_size
        with path.open("rb") as f:
            offset = max(0, size - _EOF_TAIL_WINDOW)
            f.seek(offset)
            tail = f.read()
        return b"%%EOF" in tail
    except OSError:
        return False


def _verify_integrity(
    part_path: Path,
    *,
    expected_total_bytes: int | None,
    min_pdf_bytes: int,
) -> tuple[bool, str | None]:
    """Return (ok, fail_reason)."""
    if not part_path.is_file():
        return False, "missing_part_file"
    actual = part_path.stat().st_size
    if actual < int(min_pdf_bytes):
        return False, f"too_small({actual}<{min_pdf_bytes})"
    if not is_pdf_file_valid(part_path):
        return False, "bad_pdf_header"
    if expected_total_bytes is not None and actual != int(expected_total_bytes):
        return False, f"content_length_mismatch(actual={actual},expected={expected_total_bytes})"
    if not _has_eof_marker(part_path):
        return False, "missing_eof_marker"
    return True, None


def _safe_unlink(path: Path) -> None:
    try:
        if path.exists():
            path.unlink()
    except OSError as ex:
        LOGGER.debug("unlink failed %s: %s", path, ex)


def _log_attempt(
    *,
    doc_id: str,
    url: str,
    host: str,
    attempt: int,
    resumed: bool,
    status_code: int | None,
    bytes_downloaded: int,
    content_length: int | None,
    elapsed_seconds: float,
    error_class: str | None,
    retrying: bool,
) -> None:
    LOGGER.info(
        "download_attempt | doc_id=%s host=%s attempt=%s resumed=%s status=%s "
        "bytes=%s content_length=%s elapsed=%.2fs error=%s retrying=%s url=%s",
        doc_id,
        host,
        attempt,
        resumed,
        status_code,
        bytes_downloaded,
        content_length,
        elapsed_seconds,
        error_class,
        retrying,
        url,
    )


def _is_non_pdf_content_type(content_type: str | None) -> bool:
    if not content_type:
        return False
    ct = content_type.lower()
    return any(hint in ct for hint in _NON_PDF_CONTENT_HINTS)


def download_pdf_to_path(
    session: requests.Session,
    url: str,
    target_path: Path,
    cfg: SemiStructuredIngestionConfig,
    *,
    doc_id: str = "",
    rate_limit_wait: Callable[[], None] | None = None,
    extra_headers: dict[str, str] | None = None,
) -> DownloadResult:
    """Tải 1 file PDF về `target_path` bằng cách stream vào `.part` rồi atomic rename.

    Toàn bộ retry/backoff/integrity/resume nằm ở đây để giữ ingestor mỏng.
    `rate_limit_wait` (nếu có) được gọi trước MỖI HTTP attempt (bao gồm lần đầu).
    """
    target_path = Path(target_path)
    ensure_dir(target_path.parent)
    part_path = target_path.with_suffix(target_path.suffix + ".part")
    host = host_of(url)
    timeout = timeout_for_url(url)
    verify = verify_for_url(url, cfg)
    max_attempts = max(1, int(cfg.download_retry_max_attempts))

    overall_start = time.monotonic()
    result = DownloadResult()

    last_status: int | None = None
    last_error_class: str | None = None
    last_error_msg: str | None = None
    last_expected_total: int | None = None
    delete_part_at_end: bool = False  # set True nếu lỗi không retryable kiểu non-pdf/4xx fail-fast

    for attempt in range(1, max_attempts + 1):
        result.attempts = attempt
        is_last_attempt = attempt >= max_attempts
        retrying = not is_last_attempt

        if rate_limit_wait is not None:
            try:
                rate_limit_wait()
            except Exception as ex:  # noqa: BLE001
                LOGGER.debug("rate_limit_wait raised: %s", ex)

        existing_size = part_path.stat().st_size if part_path.exists() else 0
        headers: dict[str, str] = dict(extra_headers or {})
        resumed_this_attempt = existing_size > 0
        if resumed_this_attempt:
            headers["Range"] = f"bytes={existing_size}-"

        attempt_start = time.monotonic()
        status_code: int | None = None
        content_length_header: int | None = None
        bytes_this_attempt = 0
        expected_total: int | None = None

        try:
            with session.get(
                url,
                headers=headers or None,
                stream=True,
                timeout=timeout,
                verify=verify,
                allow_redirects=True,
            ) as resp:
                status_code = resp.status_code
                cl_raw = resp.headers.get("Content-Length")
                content_length_header = int(cl_raw) if cl_raw and cl_raw.isdigit() else None
                content_type = resp.headers.get("Content-Type")

                # --- Resume edge cases -------------------------------------------------
                if resumed_this_attempt and status_code == 200:
                    LOGGER.info(
                        "resume_not_supported -> restart | doc_id=%s host=%s existing=%s",
                        doc_id,
                        host,
                        existing_size,
                    )
                    _safe_unlink(part_path)
                    existing_size = 0
                    resumed_this_attempt = False

                if resumed_this_attempt and status_code == 416:
                    LOGGER.info(
                        "range_not_satisfiable -> reset part | doc_id=%s host=%s existing=%s",
                        doc_id,
                        host,
                        existing_size,
                    )
                    _safe_unlink(part_path)
                    last_status = status_code
                    last_error_class = "RangeNotSatisfiable"
                    last_error_msg = "http_416_range_invalid"
                    _log_attempt(
                        doc_id=doc_id,
                        url=url,
                        host=host,
                        attempt=attempt,
                        resumed=True,
                        status_code=status_code,
                        bytes_downloaded=0,
                        content_length=content_length_header,
                        elapsed_seconds=time.monotonic() - attempt_start,
                        error_class=last_error_class,
                        retrying=retrying,
                    )
                    if retrying:
                        _sleep_backoff(attempt)
                        continue
                    break

                # --- HTTP status routing ----------------------------------------------
                if status_code in _FAIL_FAST_STATUS:
                    last_status = status_code
                    last_error_class = f"HTTP{status_code}"
                    last_error_msg = f"http_status_{status_code}"
                    delete_part_at_end = True
                    _log_attempt(
                        doc_id=doc_id,
                        url=url,
                        host=host,
                        attempt=attempt,
                        resumed=resumed_this_attempt,
                        status_code=status_code,
                        bytes_downloaded=0,
                        content_length=content_length_header,
                        elapsed_seconds=time.monotonic() - attempt_start,
                        error_class=last_error_class,
                        retrying=False,
                    )
                    break

                if status_code in _RETRYABLE_STATUS:
                    last_status = status_code
                    last_error_class = f"HTTP{status_code}"
                    last_error_msg = f"http_status_{status_code}"
                    retry_after = _parse_retry_after(resp.headers.get("Retry-After"))
                    _log_attempt(
                        doc_id=doc_id,
                        url=url,
                        host=host,
                        attempt=attempt,
                        resumed=resumed_this_attempt,
                        status_code=status_code,
                        bytes_downloaded=0,
                        content_length=content_length_header,
                        elapsed_seconds=time.monotonic() - attempt_start,
                        error_class=last_error_class,
                        retrying=retrying,
                    )
                    if retrying:
                        if retry_after is not None:
                            time.sleep(min(retry_after, _BACKOFF_CAP * 2))
                        else:
                            _sleep_backoff(attempt)
                        continue
                    break

                if status_code not in (200, 206):
                    last_status = status_code
                    last_error_class = f"HTTP{status_code}"
                    last_error_msg = f"unexpected_http_status_{status_code}"
                    _log_attempt(
                        doc_id=doc_id,
                        url=url,
                        host=host,
                        attempt=attempt,
                        resumed=resumed_this_attempt,
                        status_code=status_code,
                        bytes_downloaded=0,
                        content_length=content_length_header,
                        elapsed_seconds=time.monotonic() - attempt_start,
                        error_class=last_error_class,
                        retrying=retrying,
                    )
                    if retrying:
                        _sleep_backoff(attempt)
                        continue
                    break

                # --- Content-Type sanity ----------------------------------------------
                if _is_non_pdf_content_type(content_type):
                    last_status = status_code
                    last_error_class = "NonPdfContent"
                    last_error_msg = f"content_type={content_type}"
                    delete_part_at_end = True
                    _log_attempt(
                        doc_id=doc_id,
                        url=url,
                        host=host,
                        attempt=attempt,
                        resumed=resumed_this_attempt,
                        status_code=status_code,
                        bytes_downloaded=0,
                        content_length=content_length_header,
                        elapsed_seconds=time.monotonic() - attempt_start,
                        error_class=last_error_class,
                        retrying=False,
                    )
                    break

                # --- Stream-to-disk ---------------------------------------------------
                if status_code == 206:
                    mode = "ab"
                    expected_total = (
                        existing_size + content_length_header if content_length_header is not None else None
                    )
                else:
                    # 200 (full body): luôn ghi mới
                    _safe_unlink(part_path)
                    existing_size = 0
                    mode = "wb"
                    expected_total = content_length_header

                last_expected_total = expected_total

                with part_path.open(mode) as f:
                    for chunk in resp.iter_content(chunk_size=_CHUNK_SIZE):
                        if not chunk:
                            continue
                        f.write(chunk)
                        bytes_this_attempt += len(chunk)

                last_status = status_code

        except Exception as ex:  # noqa: BLE001
            retryable, exc_class = _classify_exception(ex)
            last_error_class = exc_class
            last_error_msg = f"{exc_class}: {ex}"
            if status_code is not None:
                last_status = status_code
            elapsed = time.monotonic() - attempt_start
            _log_attempt(
                doc_id=doc_id,
                url=url,
                host=host,
                attempt=attempt,
                resumed=resumed_this_attempt,
                status_code=status_code,
                bytes_downloaded=bytes_this_attempt,
                content_length=content_length_header,
                elapsed_seconds=elapsed,
                error_class=exc_class,
                retrying=retryable and retrying,
            )
            if retryable and retrying:
                _sleep_backoff(attempt)
                continue
            # Hết retry hoặc lỗi không retryable: giữ .part nếu là lỗi mạng/interrupted
            break

        # ------- Attempt hoàn tất không exception ---------------------------------
        elapsed = time.monotonic() - attempt_start
        actual_bytes = part_path.stat().st_size if part_path.exists() else 0

        # 1) Check content-length nếu server có trả
        if expected_total is not None and actual_bytes != expected_total:
            last_error_class = "ContentLengthMismatch"
            last_error_msg = (
                f"content_length_mismatch(actual={actual_bytes},expected={expected_total})"
            )
            _log_attempt(
                doc_id=doc_id,
                url=url,
                host=host,
                attempt=attempt,
                resumed=resumed_this_attempt,
                status_code=status_code,
                bytes_downloaded=bytes_this_attempt,
                content_length=content_length_header,
                elapsed_seconds=elapsed,
                error_class=last_error_class,
                retrying=retrying,
            )
            # Có khả năng server cắt giữa chừng -> reset .part để lần sau full restart
            _safe_unlink(part_path)
            if retrying:
                _sleep_backoff(attempt)
                continue
            break

        # 2) Verify integrity (header + EOF marker + size>=min)
        ok, reason = _verify_integrity(
            part_path,
            expected_total_bytes=expected_total,
            min_pdf_bytes=int(cfg.min_pdf_bytes),
        )
        if ok:
            try:
                os.replace(part_path, target_path)
            except OSError as ex:
                last_error_class = "AtomicRenameFailed"
                last_error_msg = f"{type(ex).__name__}: {ex}"
                _log_attempt(
                    doc_id=doc_id,
                    url=url,
                    host=host,
                    attempt=attempt,
                    resumed=resumed_this_attempt,
                    status_code=status_code,
                    bytes_downloaded=bytes_this_attempt,
                    content_length=content_length_header,
                    elapsed_seconds=elapsed,
                    error_class=last_error_class,
                    retrying=retrying,
                )
                if retrying:
                    _sleep_backoff(attempt)
                    continue
                break

            result.success = True
            result.integrity_ok = True
            result.http_status = status_code
            result.content_length = expected_total if expected_total is not None else content_length_header
            result.bytes_downloaded = actual_bytes
            result.download_seconds = round(time.monotonic() - overall_start, 3)
            result.error_class = None
            result.error = None
            result.resumed = resumed_this_attempt
            result.final_path = target_path
            _log_attempt(
                doc_id=doc_id,
                url=url,
                host=host,
                attempt=attempt,
                resumed=resumed_this_attempt,
                status_code=status_code,
                bytes_downloaded=actual_bytes,
                content_length=content_length_header,
                elapsed_seconds=elapsed,
                error_class=None,
                retrying=False,
            )
            return result

        # 3) Integrity fail -> reset .part và (nếu còn budget) thử lại
        last_error_class = "IntegrityCheckFailed"
        last_error_msg = reason
        _log_attempt(
            doc_id=doc_id,
            url=url,
            host=host,
            attempt=attempt,
            resumed=resumed_this_attempt,
            status_code=status_code,
            bytes_downloaded=actual_bytes,
            content_length=content_length_header,
            elapsed_seconds=elapsed,
            error_class=last_error_class,
            retrying=retrying,
        )
        # Dữ liệu trên .part đã hỏng -> xoá để lần sau không append vào file lỗi
        _safe_unlink(part_path)
        if retrying:
            _sleep_backoff(attempt)
            continue
        break

    # --- Đã hết retry budget, build failure result ----------------------------
    if delete_part_at_end:
        _safe_unlink(part_path)

    actual_bytes = part_path.stat().st_size if part_path.exists() else 0
    result.success = False
    result.integrity_ok = False
    result.http_status = last_status
    result.content_length = last_expected_total
    result.bytes_downloaded = actual_bytes
    result.download_seconds = round(time.monotonic() - overall_start, 3)
    result.error_class = last_error_class
    result.error = last_error_msg
    result.resumed = False
    result.final_path = None
    return result
