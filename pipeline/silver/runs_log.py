from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _text_or_empty(value: object) -> str:
    if value is None:
        return ""
    return str(value)


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def write_runs_entry(
    dataset: str,
    silver_root: str | Path,
    run_partition: str | None,
    started_at: datetime,
    input_rows: int,
    output_rows: int,
    output_path: str | Path,
    latest_key: str | None = None,
    status: str = "success",
    error: str | None = None,
) -> None:
    """Append one JSON Lines audit entry for a Silver transform run."""
    if started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=timezone.utc)
    finished_at = datetime.now(timezone.utc)
    entry = {
        "run_id": str(uuid.uuid4()),
        "dataset": dataset,
        "run_partition": _text_or_empty(run_partition),
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "input_rows": int(input_rows),
        "output_rows": int(output_rows),
        "output_path": str(output_path),
        "latest_key": latest_key,
        "status": status,
        "error": error,
    }
    runs_path = Path(silver_root) / dataset / "_runs.jsonl"
    runs_path.parent.mkdir(parents=True, exist_ok=True)
    with runs_path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(entry, ensure_ascii=False, default=_json_default) + "\n")
