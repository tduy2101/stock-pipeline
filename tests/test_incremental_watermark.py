"""
Structural checks for incremental watermark / lookback SQL.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = REPO_ROOT / "transform" / "dbt"
COMPILED_PATH = (
    DBT_DIR
    / "target"
    / "compiled"
    / "stock_pipeline"
    / "models"
    / "intermediate"
    / "int_price_indicator.sql"
)


class TestWatermarkLogic:
    def test_incremental_model_parses(self):
        result = subprocess.run(
            [
                "dbt",
                "parse",
                "--project-dir",
                str(DBT_DIR),
                "--profiles-dir",
                str(DBT_DIR),
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0

    def test_lookback_90_days_in_model_sql(self):
        model_path = (
            REPO_ROOT
            / "transform"
            / "dbt"
            / "models"
            / "intermediate"
            / "int_price_indicator.sql"
        )
        model_sql = model_path.read_text(encoding="utf-8")
        assert "90 days" in model_sql or "interval '90" in model_sql, (
            "int_price_indicator should contain 90-day lookback window"
        )
