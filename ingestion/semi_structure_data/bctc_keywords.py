"""Heuristic tokens for filtering financial-report PDFs from disclosure HTML."""

DEFAULT_BCTC_KEYWORDS: tuple[str, ...] = (
    "bctc",
    "bctn",
    "financial statement",
    "bao cao tai chinh",
    "bao cao thuong nien",
    "kiem toan",
    "hop nhat",
    "soat xet",
    "tai chinh",
    # UTF-8 Vietnamese (filenames / titles often use accents)
    "b\u00e1o c\u00e1o t\u00e0i ch\u00ednh",
    "b\u00e1o c\u00e1o th\u01b0\u1eddng ni\u00ean",
    "h\u1ee3p nh\u1ea5t",
    "ki\u1ec3m to\u00e1n",
    "so\u00e1t x\u00e9t",
)


def _norm_blob(s: str) -> str:
    return s.lower()


def _slug_suggests_financial_pdf(url: str) -> bool:
    """
    Tên file PDF công bố HSX/FII thường không ghi ``bctc`` mà dùng slug ASCII:
    ``tai-chinh``, ``kiem-toan``, ``hop-nhat``, ``thuong-nien``, ``--cbtt--``, …
    """
    u = (url or "").lower()
    if ".pdf" not in u:
        return False
    slug_hits = (
        "bctc",
        "bctn",
        "tai-chinh",
        "bao-cao-tai-chinh",
        "bc-tc",
        "kiem-toan",
        "soat-xet",
        "hop-nhat",
        "thuong-nien",
        "bao-cao-tai-chinh-hop-nhat",
        "bao-cao-tinh-hinh-tai-chinh",
        "-q1-20",
        "-q2-20",
        "-q3-20",
        "-q4-20",
        "quy-1-20",
        "quy-2-20",
        "quy-3-20",
        "quy-4-20",
    )
    if any(t in u for t in slug_hits):
        return True
    if "cbtt" in u and any(t in u for t in ("thuong-nien", "tai-chinh", "kiem-toan", "hop-nhat", "soat-xet")):
        return True
    return False


def is_bctc_candidate(title: str, pdf_url: str, extra: tuple[str, ...] | None = None) -> bool:
    blob = _norm_blob(f"{title or ''} {pdf_url or ''}")
    tokens = DEFAULT_BCTC_KEYWORDS if not extra else tuple(dict.fromkeys([*DEFAULT_BCTC_KEYWORDS, *extra]))
    if any(_norm_blob(k) in blob for k in tokens):
        return True
    u = (pdf_url or "").lower()
    if "bctn" in u or "bctc" in u or "tai-chinh" in u:
        return True
    if _slug_suggests_financial_pdf(pdf_url):
        return True
    return False
