from __future__ import annotations

import hashlib
import re
from pathlib import Path


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def sha256_file(path: Path, chunk_size: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def is_pdf_file_valid(path: Path) -> bool:
    if not path.is_file():
        return False
    try:
        with path.open("rb") as f:
            header = f.read(5)
        return header == b"%PDF-"
    except OSError:
        return False


def safe_filename(value: str, max_len: int = 140) -> str:
    clean = re.sub(r"[^A-Za-z0-9._-]+", "_", (value or "").strip())
    clean = clean.strip("._")
    if not clean:
        clean = "unknown"
    return clean[:max_len]

