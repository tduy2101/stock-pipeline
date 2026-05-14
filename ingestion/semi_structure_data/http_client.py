"""Shared HTTP client helpers for BCTC PDF ingestion.

Mục tiêu:
- Cung cấp `requests.Session` dùng chung cho toàn bộ stage download (keep-alive,
  pool connection ổn định hơn so với việc tạo socket/SSL handshake cho từng file).
- Adapter retry để mức tối thiểu (max_retries=0): toàn bộ retry/backoff sẽ do
  `downloader.py` quản (tránh "double retry" giữa urllib3 và app-level).
- Chuẩn hoá logic SSL verify cho host HNX (giữ tương thích với hành vi cũ).
- Chuẩn hoá timeout tuple (connect, read) theo host:
    * owa.hnx.vn  -> read 180s (file PDF lớn, server hay ngắt stream)
    * còn lại     -> read 90s
  Đều có thể override qua env.

Không thay đổi public API của package.
"""

from __future__ import annotations

import logging
import os
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter

from .config import SemiStructuredIngestionConfig

LOGGER = logging.getLogger(__name__)

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36 stock-pipeline-bctc/1.0"
)

DEFAULT_DOWNLOAD_HEADERS: dict[str, str] = {
    "User-Agent": DEFAULT_USER_AGENT,
    "Accept": "application/pdf,*/*;q=0.8",
    "Accept-Encoding": "identity",
    "Connection": "keep-alive",
}

DEFAULT_CONNECT_TIMEOUT = 10.0
DEFAULT_READ_TIMEOUT = 90.0
HNX_OWA_READ_TIMEOUT = 180.0

_HNX_OWA_HOST = "owa.hnx.vn"


def host_of(url: str) -> str:
    return (urlparse(url).hostname or "").lower()


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        LOGGER.debug("env %s khong phai float: %r (dung default %s)", name, raw, default)
        return default


def timeout_for_url(url: str) -> tuple[float, float]:
    """Return (connect_timeout, read_timeout) phù hợp với host.

    Host `owa.hnx.vn` (host phục vụ attachment HNX) được cấp read timeout dài hơn
    do thường stream chậm và dễ bị ngắt. Có thể override bằng env:
    - BCTC_DOWNLOAD_CONNECT_TIMEOUT
    - BCTC_DOWNLOAD_READ_TIMEOUT  (host khác)
    - BCTC_DOWNLOAD_READ_TIMEOUT_HNX_OWA  (riêng owa.hnx.vn)
    """
    host = host_of(url)
    connect_to = _env_float("BCTC_DOWNLOAD_CONNECT_TIMEOUT", DEFAULT_CONNECT_TIMEOUT)
    if host == _HNX_OWA_HOST:
        read_to = _env_float("BCTC_DOWNLOAD_READ_TIMEOUT_HNX_OWA", HNX_OWA_READ_TIMEOUT)
    else:
        read_to = _env_float("BCTC_DOWNLOAD_READ_TIMEOUT", DEFAULT_READ_TIMEOUT)
    return (connect_to, read_to)


def _certifi_bundle() -> bool | str:
    try:
        import certifi

        return certifi.where()
    except ImportError:
        return True


def _silence_urllib3_insecure() -> None:
    try:
        import urllib3

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    except Exception:  # noqa: BLE001
        pass


def verify_for_url(url: str, cfg: SemiStructuredIngestionConfig) -> bool | str:
    """SSL verify policy cho HNX host (giữ tương thích hành vi cũ).

    Thứ tự ưu tiên: HNX_SSL_VERIFY env -> cfg.hnx_verify_ssl -> certifi.
    Host không thuộc *.hnx.vn luôn verify với CA bundle mặc định.
    """
    host = host_of(url)
    if not host.endswith("hnx.vn"):
        return True

    env = os.environ.get("HNX_SSL_VERIFY", "").strip().lower()
    if env in ("0", "false", "no"):
        _silence_urllib3_insecure()
        return False
    if env in ("1", "true", "yes"):
        return _certifi_bundle()
    if not cfg.hnx_verify_ssl:
        _silence_urllib3_insecure()
        return False
    return _certifi_bundle()


def make_session(extra_headers: dict[str, str] | None = None) -> requests.Session:
    """Khởi tạo session dùng chung cho toàn bộ stage download.

    - pool_connections / pool_maxsize cấp đủ cho 1 vài host (HNX www + owa).
    - max_retries=0: KHÔNG cho urllib3 tự retry, downloader sẽ tự quản.
    """
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=16, pool_maxsize=16, max_retries=0)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    headers = dict(DEFAULT_DOWNLOAD_HEADERS)
    if extra_headers:
        headers.update(extra_headers)
    session.headers.update(headers)
    return session
