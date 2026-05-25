from __future__ import annotations

import re
import unicodedata

import pandas as pd

NULLISH_TEXT = {"", "nan", "none", "null", "<na>"}


def is_nullish(value: object) -> bool:
    if value is None or pd.isna(value):
        return True
    return str(value).strip().lower() in NULLISH_TEXT


def normalize_text(value: object) -> object:
    if is_nullish(value):
        return pd.NA
    text = unicodedata.normalize("NFC", str(value))
    text = re.sub(r"\s+", " ", text).strip()
    if text.lower() in NULLISH_TEXT:
        return pd.NA
    return text


def normalize_text_series(series: pd.Series) -> pd.Series:
    return series.map(normalize_text).astype("string")


def upper_nullable(value: object) -> object:
    if is_nullish(value):
        return pd.NA
    return str(value).strip().upper()


def lower_nullable(value: object, *, default: str | None = None) -> object:
    if is_nullish(value):
        return default if default is not None else pd.NA
    return str(value).strip().lower()


def coerce_bool_series(series: pd.Series, *, default: bool = False) -> pd.Series:
    truthy = {"1", "true", "t", "yes", "y"}
    falsy = {"0", "false", "f", "no", "n"}

    def _one(value: object) -> bool:
        if value is None or pd.isna(value):
            return default
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if text in truthy:
            return True
        if text in falsy or text in NULLISH_TEXT:
            return False
        return bool(value)

    return series.map(_one).astype("bool")


def has_text(value: object) -> bool:
    return not is_nullish(value)
