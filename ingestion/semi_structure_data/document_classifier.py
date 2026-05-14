"""Rule-based document classification + canonical selection for HNX BCTC PDF flow.

Không dùng ML/LLM. Chuẩn hoá title (ASCII, lowercase, collapse spaces) rồi áp rule.
Public API ingest/pipeline không đổi; module này được gọi từ provider + ingestor + parser.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from hashlib import sha1
from pathlib import Path
from typing import Any, Literal

from .config import SemiStructuredIngestionConfig

DocClass = Literal[
    "financial_statement_consolidated",
    "financial_statement_separate",
    "explanation",
    "disclosure",
    "announcement",
    "unknown",
]
Language = Literal["VI", "EN", "UNKNOWN"]

_YEAR_RE = re.compile(r"20\d{2}")

_FINANCIAL_DOC_CLASSES = (
    "financial_statement_consolidated",
    "financial_statement_separate",
)

# NOISE — không parse (giữ crawl/download nếu pipeline vẫn tải)
_NOISE_EXPLANATION = (
    "giai trinh",
    "explanation",
    "explanations",
    "relating to fss",
    "relating to the fss",
)
_NOISE_DISCLOSURE = (
    "cbtt",
    "cong bo thong tin",
    "disclosure",
    "cv_",  # công văn / CV prefix trong tên file
)
_NOISE_ANNOUNCEMENT = (
    "nghi quyet",
    "resolution",
)

# CANONICAL — BCTC hợp nhất / riêng
_CONSOLIDATED = (
    "hop nhat",
    "consolidated",
    "consolidated financial statements",
)
_SEPARATE = (
    "bao cao tai chinh",
    "financial statements",
    "cong ty me",
    "separate financial statements",
    "parent company",
)

_VI_MARKERS_NORM = (
    "bao cao tai chinh",
    "tai chinh",
    "kiem toan",
    "da kiem toan",
    "hop nhat",
    "giai trinh",
    "quy",
    "nam",
    "ban nien",
    "thang",
    "tong cong ty",
    "cong ty me",
)

_EN_MARKERS_NORM = (
    "financial statements",
    "consolidated financial statements",
    "separate financial statements",
    "quarterly report",
    "annual report",
    "reviewed interim",
    "audited",
    "independent auditor",
)


def strip_vietnamese_accents(text: str) -> str:
    t = (text or "").lower()
    repl = (
        ("áàảãạăắằẳẵặâấầẩẫậ", "a"),
        ("éèẻẽẹêếềểễệ", "e"),
        ("íìỉĩị", "i"),
        ("óòỏõọôốồổỗộơớờởỡợ", "o"),
        ("úùủũụưứừửữự", "u"),
        ("ýỳỷỹỵ", "y"),
        ("đ", "d"),
    )
    for grp, ch in repl:
        for c in grp:
            t = t.replace(c, ch)
    return t


def normalize_title_for_classify(text: str | None) -> str:
    """Lowercase, bỏ dấu tiếng Việt, _-. → space, collapse spaces."""
    if not text:
        return ""
    t = strip_vietnamese_accents(str(text))
    t = t.lower().strip()
    for sep in ("_", "-", "."):
        t = t.replace(sep, " ")
    t = re.sub(r"[^a-z0-9\s]+", " ", t)
    return " ".join(t.split())


def has_vietnamese_script(text: str | None) -> bool:
    if not text:
        return False
    for ch in str(text):
        o = ord(ch)
        if 0x1E00 <= o <= 0x1EFF:  # Latin Extended Additional (Vietnamese letters)
            return True
        if ch in "àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổờơớợởỡùúụủũưừứựửữỳýỵỷỹđĐ":
            return True
    return False


def detect_language(
    raw_title: str | None,
    raw_attachment: str | None,
    url_pdf: str | None,
) -> Language:
    """Heuristic VI vs EN từ title + tên file + basename URL."""
    combined_raw = " ".join(
        x for x in (raw_title or "", raw_attachment or "", Path(url_pdf or "").name) if x
    )
    norm = normalize_title_for_classify(combined_raw)
    basename = Path(url_pdf or "").name
    # HNX thường đặt tên file: ..._VI_... / ..._EN_... (ưu tiên trước marker chữ).
    if basename and re.search(r"(?i)[_.-]vi[_.-]", basename):
        return "VI"
    if basename and re.search(r"(?i)[_.-]en[_.-]", basename) and not has_vietnamese_script(combined_raw):
        return "EN"
    if has_vietnamese_script(combined_raw):
        return "VI"
    if any(m in norm for m in _EN_MARKERS_NORM) and not any(m in norm for m in _VI_MARKERS_NORM):
        return "EN"
    if any(m in norm for m in _VI_MARKERS_NORM):
        return "VI"
    return "UNKNOWN"


def _first_matching_noise(norm: str) -> tuple[DocClass, bool] | None:
    if any(k in norm for k in _NOISE_EXPLANATION):
        return "explanation", True
    if any(k in norm for k in _NOISE_DISCLOSURE):
        return "disclosure", True
    if any(k in norm for k in _NOISE_ANNOUNCEMENT):
        return "announcement", True
    return None


def classify_doc_class(norm: str) -> DocClass:
    noise = _first_matching_noise(norm)
    if noise:
        return noise[0]
    if any(k in norm for k in _CONSOLIDATED):
        return "financial_statement_consolidated"
    if any(k in norm for k in _SEPARATE):
        return "financial_statement_separate"
    return "unknown"


def _is_financial_statement(doc_class: str) -> bool:
    return doc_class in _FINANCIAL_DOC_CLASSES


def infer_period_key(
    norm: str,
    year_hint: str | None,
    title: str,
    url_pdf: str | None,
) -> str:
    """Khóa logic (ticker + period) để dedupe canonical."""
    y = (year_hint or "").strip()
    if not y or y.upper() == "UNKNOWN":
        m = _YEAR_RE.search(title) or _YEAR_RE.search(url_pdf or "")
        y = m.group(0) if m else "UNKNOWN"
    # Quý
    if re.search(r"\bq\s*1\b|\bquy\s*1\b|\bquy\s*i\b", norm):
        return f"{y}-Q1"
    if re.search(r"\bq\s*2\b|\bquy\s*2\b|\bquy\s*ii\b", norm):
        return f"{y}-Q2"
    if re.search(r"\bq\s*3\b|\bquy\s*3\b|\bquy\s*iii\b", norm):
        return f"{y}-Q3"
    if re.search(r"\bq\s*4\b|\bquy\s*4\b|\bquy\s*iv\b", norm):
        return f"{y}-Q4"
    if "6 thang" in norm or "ban nien" in norm or "half year" in norm or "interim 6" in norm:
        return f"{y}-H1"
    if "9 thang" in norm or "nine month" in norm:
        return f"{y}-9M"
    if "nam" in norm or "annual" in norm or "year ended" in norm or "kiem toan nam" in norm:
        return f"{y}-ANNUAL"
    return f"{y}-GENERAL"


def canonical_priority(
    doc_class: DocClass,
    language: Language,
    *,
    allow_en: bool,
) -> int:
    """Số nhỏ = ưu tiên cao cho chọn canonical. 99 = không candidate."""
    if not _is_financial_statement(doc_class):
        return 99
    if language == "EN" and not allow_en:
        return 99
    if language == "EN" and allow_en:
        return 5 if doc_class == "financial_statement_consolidated" else 6
    if language == "UNKNOWN":
        # Thị trường VN: mặc định coi UNKNOWN + canonical như candidate (ưu tiên sau VI rõ)
        return 3 if doc_class == "financial_statement_consolidated" else 4
    if doc_class == "financial_statement_consolidated":
        return 1
    return 2


def build_stable_doc_id(doc: dict[str, Any]) -> str:
    """Trùng khớp logic doc_id trong ingestor (ổn định log/metadata)."""
    key = "|".join(
        [
            str(doc.get("source") or ""),
            str(doc.get("ticker") or ""),
            str(doc.get("title") or ""),
            str(doc.get("published_at") or ""),
            str(doc.get("url_pdf") or ""),
        ]
    )
    return sha1(key.encode("utf-8")).hexdigest()


def _allow_en(cfg: SemiStructuredIngestionConfig) -> bool:
    return bool(cfg.resolved_allow_en_docs_for_parse())


def should_download_financial_pdf(
    doc: dict[str, Any], cfg: SemiStructuredIngestionConfig
) -> tuple[bool, str | None]:
    """Quyết định có tải PDF hay không.

    Khi ``ingest_only_financial_statement_vi`` bật (mặc định): chỉ tải BCTC hợp nhất / riêng
    bản tiếng Việt (hoặc UNKNOWN nếu ``ingest_unknown_language_financial``). Loại EN, giải trình,
    CBTT, unknown class — vẫn ghi metadata với status skipped.

    Trả về (True, None) nếu tải; (False, reason) nếu bỏ qua.
    """
    if not cfg.resolved_ingest_only_financial_statement_vi():
        return True, None
    dclass = str(doc.get("doc_class") or "unknown")
    if not _is_financial_statement(dclass):
        return False, f"skipped_doc_class:{dclass}"
    lang = str(doc.get("language") or "UNKNOWN").upper()
    allow_en = _allow_en(cfg)
    if lang == "VI":
        return True, None
    if lang == "EN":
        return (True, None) if allow_en else (False, "skipped_language_en")
    if lang == "UNKNOWN":
        return (True, None) if cfg.ingest_unknown_language_financial else (
            False,
            "skipped_language_unknown",
        )
    return False, "skipped_language"


@dataclass
class ClassificationResult:
    normalized_title: str
    doc_class: DocClass
    language: Language
    is_consolidated: bool
    is_explanation: bool
    is_disclosure: bool
    canonical_priority: int
    keep_for_parse: bool
    period_key: str


def classify_record(doc: dict[str, Any], cfg: SemiStructuredIngestionConfig) -> ClassificationResult:
    """Gán đầy đủ field classification cho một record (chưa dedupe canonical theo nhóm)."""
    title = str(doc.get("title") or "").strip()
    att = str(doc.get("attachment_name") or "").strip()
    url = str(doc.get("url_pdf") or "").strip()
    combined = f"{title} {att}".strip()
    norm = normalize_title_for_classify(f"{combined} {Path(url).name}")
    lang = detect_language(title, att or None, url or None)
    dclass = classify_doc_class(norm)
    allow_en = _allow_en(cfg)

    is_consolidated = dclass == "financial_statement_consolidated"
    is_explanation = dclass == "explanation"
    is_disclosure = dclass == "disclosure"

    pri = canonical_priority(dclass, lang, allow_en=allow_en)
    year_hint = str(doc.get("year") or "").strip() or None
    pkey = infer_period_key(norm, year_hint, title, url)

    # Candidate parse: canonical financial + không phải noise class + ngôn ngữ OK
    keep = pri < 99 and dclass in (
        "financial_statement_consolidated",
        "financial_statement_separate",
    )
    if lang == "EN" and not allow_en:
        keep = False
    # UNKNOWN: vẫn parse nếu là BCTC canonical (tránh drop file chỉ có ASCII tên)
    if lang == "UNKNOWN" and keep:
        pass

    return ClassificationResult(
        normalized_title=norm,
        doc_class=dclass,
        language=lang,
        is_consolidated=is_consolidated,
        is_explanation=is_explanation,
        is_disclosure=is_disclosure,
        canonical_priority=pri,
        keep_for_parse=keep,
        period_key=pkey,
    )


def enrich_document_row(doc: dict[str, Any], cfg: SemiStructuredIngestionConfig) -> dict[str, Any]:
    """Mutate / trả về doc kèm field classification (default đầy đủ)."""
    c = classify_record(doc, cfg)
    out = dict(doc)
    out["normalized_title"] = c.normalized_title
    out["doc_class"] = c.doc_class
    out["language"] = c.language
    out["is_consolidated"] = bool(c.is_consolidated)
    out["is_explanation"] = bool(c.is_explanation)
    out["is_disclosure"] = bool(c.is_disclosure)
    out["canonical_priority"] = int(c.canonical_priority)
    out["keep_for_parse"] = bool(c.keep_for_parse)
    out["period_key"] = c.period_key
    return out


def apply_canonical_selection(docs: list[dict[str, Any]], cfg: SemiStructuredIngestionConfig) -> None:
    """Trong mỗi nhóm (ticker, period_key), chỉ giữ 1 bản canonical VI (ưu tiên consolidated).

    Ghi đè keep_for_parse trên list in-place.
    """
    allow_en = _allow_en(cfg)
    # Chỉ các doc đang candidate mới tham gia chọn
    groups: dict[tuple[str, str], list[tuple[int, dict[str, Any], int, str]]] = {}
    for idx, d in enumerate(docs):
        if not d.get("keep_for_parse"):
            continue
        ticker = str(d.get("ticker") or "").strip().upper() or "UNKNOWN"
        pkey = str(d.get("period_key") or "UNKNOWN")
        pri = int(d.get("canonical_priority") or 99)
        url = str(d.get("url_pdf") or "")
        if pri >= 99:
            continue
        if str(d.get("language")) == "EN" and not allow_en:
            d["keep_for_parse"] = False
            continue
        groups.setdefault((ticker, pkey), []).append((idx, d, pri, url))

    for _gkey, members in groups.items():
        # Ưu tiên pri nhỏ nhất, tie-break url_pdf tăng dần (ổn định)
        members.sort(key=lambda x: (x[2], x[3]))
        for i, (idx, d, pri, url) in enumerate(members):
            if i == 0:
                d["keep_for_parse"] = True
            else:
                d["keep_for_parse"] = False

    # Đảm bảo noise / EN / unknown-class không parse
    for d in docs:
        dclass = str(d.get("doc_class") or "unknown")
        lang = str(d.get("language") or "UNKNOWN")
        if not _is_financial_statement(dclass):
            d["keep_for_parse"] = False
        if lang == "EN" and not allow_en:
            d["keep_for_parse"] = False
