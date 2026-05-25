"""Shared ticker mention detection for news Bronze and Silver."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

# Symbols that collide with common Vietnamese / finance tokens in news text.
TICKER_BLOCKLIST: frozenset[str] = frozenset(
    {
        "CEO",
        "HCM",
        "LAI",
        "NHA",
        "THU",
        "TIN",
        "TOP",
        "TRA",
        "USD",
        "VUA",
        "VND",
    }
)

_EXCHANGE_HINT = r"(?:HOSE|HNX|UPCOM|UPCOMINDEX|HSX)"

_WS_RE = re.compile(r"\s+")
_LOCAL_CONTEXT_CHARS = 56
_PART_BASE_SCORES = (80, 40, 0)

_EXCHANGE_PREFIX_RE = re.compile(rf"\b{_EXCHANGE_HINT}\s*:?\s*$")
_QUALIFIED_CODE_PREFIX_RE = re.compile(
    r"\b(?:MCK|MA\s+CK|MA\s+CHUNG\s+KHOAN)\s*:?\s*$"
)
_PLAIN_CODE_PREFIX_RE = re.compile(r"\bMA\s*:?\s*$")
_EQUITY_PREFIX_RE = re.compile(r"\b(?:CP|CO\s+PHIEU|CHUNG\s+KHOAN)\s+$")
_MARKET_MOVE_SUFFIX_RE = re.compile(r"^\s*[-:]\s*[\d+%+-]")

_NEGATIVE_PATTERNS: dict[str, tuple[str, ...]] = {
    "HCM": (
        r"\bTP\s*\.?\s*HCM\b",
        r"\bTPHCM\b",
        r"\bHO\s+CHI\s+MINH\b",
        r"\bTHANH\s+PHO\s+HO\s+CHI\s+MINH\b",
    ),
    "LAI": (
        r"\bGIA\s+LAI\b",
        r"\bLAI\s+(?:SUAT|RONG|VAY|TIEN|SAU|TRUOC|LON|GOP|KEP)\b",
        r"\bCO\s+LAI\b",
    ),
    "NHA": (
        r"\bNHA\s+(?:NUOC|DAU\s+TU|TY\s+PHU|DAT|O|MAY|XUONG|BANG)\b",
        r"\bUY\s+BAN\s+CHUNG\s+KHOAN\s+NHA\s+NUOC\b",
    ),
    "THU": (
        r"\bTHU\s+\d+\b",
        r"\b(?:DOANH|LOI|NGUON|KHOAN|TIEN|THUE)\s+THU\b",
        r"\bTHU\s+(?:NHAP|HUT|HOI|PHI|VE|DUOC|TU|NGAN|NGHIEM)\b",
        r"\bHA\s+THU\b",
    ),
    "TIN": (
        r"\bTIN\s+(?:TUC|MOI|DON|HIEU|VUI|XAU|TICH\s+CUC|TIEU\s+CUC)\b",
        r"\bBAN\s+TIN\b",
    ),
    "TOP": (
        r"\bTOP\s+\d+\b",
        r"\bTOP\s+(?:CO\s+PHIEU|DOANH\s+NGHIEP|NGANH|DAU)\b",
    ),
    "TRA": (
        r"\bTRA\s+(?:CO\s+TUC|LAI|NO|TIEN|PHI|LUONG)\b",
        r"\b(?:CHI|HOAN|DUOC|PHAI|SE)\s+TRA\b",
    ),
    "USD": (
        r"\bTY\s+GIA\s+USD\b",
        r"[\d.,]+\s*(?:TY|TRIEU|NGHIN)?\s+USD\b",
        r"\b(?:TY|TRIEU|NGHIN)\s+USD\b",
        r"\bUSD\s*/\s*VND\b",
        r"\bUSD\s+(?:TANG|GIAM|MANH|YEU|LEN|XUONG|DO\s+LA|DOLA)\b",
    ),
    "VND": (
        r"\bTY\s+GIA\s+(?:USD\s*/\s*)?VND\b",
        r"[\d.,]+\s*(?:TY|TRIEU|NGHIN)?\s+VND\b",
        r"\b(?:TY|TRIEU|NGHIN)\s+VND\b",
        r"\bUSD\s*/\s*VND\b",
        r"\bVND\s*/\s*USD\b",
        r"\bDONG\s+VND\b",
    ),
    "VUA": (
        r"\bVUA\s+(?:QUA|MOI|DUOC|TANG|GIAM|CONG\s+BO|RA\s+MAT|CHO\s+BIET)\b",
    ),
}


@dataclass(frozen=True, slots=True)
class _PreparedText:
    raw: str
    folded: str
    folded_to_raw: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class _TickerCandidate:
    code: str
    score: int
    part_index: int
    start: int
    reason: str


def _prepare_text(text: Any) -> _PreparedText:
    if text is None:
        return _PreparedText("", "", ())
    try:
        if pd.isna(text):
            return _PreparedText("", "", ())
    except (TypeError, ValueError):
        pass

    raw = unicodedata.normalize("NFC", str(text)).replace("\r", " ").replace("\n", " ")
    if raw.strip().lower() in {"nan", "none", "<na>"}:
        return _PreparedText("", "", ())

    folded_parts: list[str] = []
    folded_to_raw: list[int] = []
    for raw_idx, char in enumerate(raw):
        piece = unicodedata.normalize("NFD", char)
        piece = "".join(c for c in piece if unicodedata.category(c) != "Mn").upper()
        if not piece:
            continue
        folded_parts.append(piece)
        folded_to_raw.extend([raw_idx] * len(piece))
    return _PreparedText(
        raw=raw,
        folded="".join(folded_parts),
        folded_to_raw=tuple(folded_to_raw),
    )


def fold_for_match(text: Any) -> str:
    """Normalize text for matching: NFC, strip accents, upper, collapse whitespace."""
    prepared = _prepare_text(text)
    folded = _WS_RE.sub(" ", prepared.folded).strip()
    if folded.lower() in {"nan", "none", "<na>"}:
        return ""
    return folded


def load_stock_universe(listing_path: Path) -> frozenset[str]:
    """Load stock symbols from listing parquet (security_type=stock when available)."""
    if not listing_path.is_file():
        return frozenset()
    try:
        listing = pd.read_parquet(listing_path)
    except Exception:
        return frozenset()

    if "symbol" not in listing.columns:
        return frozenset()

    out = listing.copy()
    if "security_type" in out.columns:
        st = out["security_type"].astype(str).str.strip().str.lower()
        if st.notna().any():
            out = out.loc[st.eq("stock")].copy()
    elif "id" in out.columns:
        out = out.loc[out["id"] == 1].copy()

    symbols = (
        out["symbol"]
        .astype(str)
        .str.strip()
        .str.upper()
        .replace("", pd.NA)
        .dropna()
    )
    cleaned = {s for s in symbols if s and s not in {"NAN", "NONE", "<NA>"} and len(s) >= 2}
    return frozenset(cleaned)


def _build_boundary_pattern(codes: Iterable[str]) -> re.Pattern[str] | None:
    ordered = sorted(set(codes), key=len, reverse=True)
    if not ordered:
        return None
    inner = "|".join(re.escape(code) for code in ordered)
    return re.compile(rf"(?<![A-Z0-9])(?:{inner})(?![A-Z0-9])")


def _requires_context(code: str, blocklist: frozenset[str]) -> bool:
    # All 3-letter symbols collide with Vietnamese tokens; 4+ can match on boundary.
    return code in blocklist or len(code) <= 3


def _local_window(folded_text: str, start: int, end: int) -> str:
    left = max(0, start - _LOCAL_CONTEXT_CHARS)
    right = min(len(folded_text), end + _LOCAL_CONTEXT_CHARS)
    return folded_text[left:right]


def _raw_token(prepared: _PreparedText, start: int, end: int) -> str:
    if not prepared.folded_to_raw or start >= len(prepared.folded_to_raw):
        return ""
    raw_start = prepared.folded_to_raw[start]
    raw_end = prepared.folded_to_raw[end - 1] + 1
    return prepared.raw[raw_start:raw_end]


def _is_upper_token(token: str) -> bool:
    letters = [char for char in token if char.isalpha()]
    return bool(letters) and all(char == char.upper() for char in letters)


def _has_negative_context(folded_text: str, code: str, start: int, end: int) -> bool:
    patterns = _NEGATIVE_PATTERNS.get(code, ())
    if not patterns:
        return False
    window = _local_window(folded_text, start, end)
    return any(re.search(pattern, window) for pattern in patterns)


def _context_score(
    folded_text: str,
    start: int,
    end: int,
    *,
    original_upper: bool,
) -> tuple[int, str | None]:
    left = folded_text[max(0, start - _LOCAL_CONTEXT_CHARS) : start]
    right = folded_text[end : min(len(folded_text), end + _LOCAL_CONTEXT_CHARS)]

    if _EXCHANGE_PREFIX_RE.search(left):
        return 110, "exchange_prefix"
    if _QUALIFIED_CODE_PREFIX_RE.search(left):
        return 105, "qualified_code_prefix"
    if _PLAIN_CODE_PREFIX_RE.search(left) and original_upper:
        return 95, "plain_code_prefix"
    if _EQUITY_PREFIX_RE.search(left) and original_upper:
        return 85, "equity_prefix"

    left_stripped = left.rstrip()
    if left_stripped.endswith("(") and right.lstrip().startswith(")") and original_upper:
        return 80, "parenthesized"
    if _MARKET_MOVE_SUFFIX_RE.match(right) and original_upper:
        return 70, "market_move_suffix"
    if right.lstrip().startswith("(") and original_upper:
        return 55, "suffix_parenthesis"
    return 0, None


def _candidate_score(
    prepared: _PreparedText,
    match: re.Match[str],
    code: str,
    blocklist: frozenset[str],
) -> tuple[int, str | None]:
    start, end = match.span()
    if _has_negative_context(prepared.folded, code, start, end):
        return 0, None

    raw_token = _raw_token(prepared, start, end)
    original_upper = _is_upper_token(raw_token)
    context_score, reason = _context_score(
        prepared.folded,
        start,
        end,
        original_upper=original_upper,
    )
    if context_score:
        return context_score + (10 if original_upper else 0), reason

    if not _requires_context(code, blocklist):
        return 60 + (10 if original_upper else 0), "boundary"

    # Most Vietnamese stock symbols are 3 letters. A standalone all-caps token is
    # useful evidence for normal symbols, but too weak for the explicit blocklist.
    if len(code) <= 3 and code not in blocklist and original_upper:
        return 55, "uppercase_short_symbol"
    return 0, None


def _part_base_score(part_index: int) -> int:
    if part_index < len(_PART_BASE_SCORES):
        return _PART_BASE_SCORES[part_index]
    return 0


def _find_candidates(
    text: Any,
    universe: frozenset[str],
    *,
    blocklist: frozenset[str],
    part_index: int,
) -> list[_TickerCandidate]:
    prepared = _prepare_text(text)
    if not prepared.folded or not universe:
        return []

    pattern = _build_boundary_pattern(universe)
    if pattern is None:
        return []

    candidates: list[_TickerCandidate] = []
    base_score = _part_base_score(part_index)
    for match in pattern.finditer(prepared.folded):
        code = match.group(0).upper()
        if code not in universe:
            continue
        score, reason = _candidate_score(prepared, match, code, blocklist)
        if not score or reason is None:
            continue
        candidates.append(
            _TickerCandidate(
                code=code,
                score=score + base_score,
                part_index=part_index,
                start=match.start(),
                reason=reason,
            )
        )
    return candidates


def _rank_unique(candidates: Iterable[_TickerCandidate]) -> list[str]:
    best: dict[str, _TickerCandidate] = {}
    for candidate in candidates:
        current = best.get(candidate.code)
        if current is None or (
            candidate.score,
            -candidate.part_index,
            -candidate.start,
        ) > (
            current.score,
            -current.part_index,
            -current.start,
        ):
            best[candidate.code] = candidate
    ordered = sorted(
        best.values(),
        key=lambda item: (-item.score, item.part_index, item.start, item.code),
    )
    return [item.code for item in ordered]


def find_mentions(
    text: Any,
    universe: frozenset[str],
    *,
    blocklist: frozenset[str] | None = None,
) -> list[str]:
    """Return ordered unique ticker symbols mentioned in text."""
    block = blocklist if blocklist is not None else TICKER_BLOCKLIST
    return _rank_unique(_find_candidates(text, universe, blocklist=block, part_index=0))


def find_mentions_in_parts(
    parts: list[Any],
    universe: frozenset[str],
    *,
    blocklist: frozenset[str] | None = None,
) -> list[str]:
    """Find mentions across text parts; returns unique symbols ranked by confidence."""
    block = blocklist if blocklist is not None else TICKER_BLOCKLIST
    candidates: list[_TickerCandidate] = []
    for part_index, part in enumerate(parts):
        candidates.extend(
            _find_candidates(part, universe, blocklist=block, part_index=part_index)
        )
    return _rank_unique(candidates)


def pick_primary_ticker(mentions: list[str]) -> str | None:
    if not mentions:
        return None
    return mentions[0]


def build_ticker_regex(tickers: list[str]) -> re.Pattern[str] | None:
    """Backward-compatible regex builder for legacy callers (prefer find_mentions)."""
    codes = [str(t).strip().upper() for t in tickers if str(t).strip()]
    return _build_boundary_pattern(codes)


def infer_ticker_from_texts(
    texts: list[Any],
    universe: frozenset[str],
    *,
    blocklist: frozenset[str] | None = None,
) -> str | None:
    return pick_primary_ticker(find_mentions_in_parts(texts, universe, blocklist=blocklist))
