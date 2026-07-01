"""
Tests confirming incremental dbt models keep expected config contracts.
Run `dbt compile --profiles-dir transform/dbt` before manifest-based tests.
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = REPO_ROOT / "transform" / "dbt"
MANIFEST_PATH = DBT_DIR / "target" / "manifest.json"


def run_dbt(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["dbt", *args, "--profiles-dir", str(DBT_DIR)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


class TestDbtIncremental:
    def test_dbt_parse_no_errors(self):
        result = run_dbt(["parse", "--project-dir", str(DBT_DIR)])
        assert result.returncode == 0, (
            f"dbt parse failed:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"
        )

    def test_incremental_models_have_correct_strategy(self):
        if not MANIFEST_PATH.is_file():
            pytest.skip("Run dbt compile first")

        with MANIFEST_PATH.open(encoding="utf-8") as handle:
            manifest = json.load(handle)

        target_models = [
            "model.stock_pipeline.int_price_indicator",
            "model.stock_pipeline.fact_price_daily",
            "model.stock_pipeline.mart_stock_daily",
            "model.stock_pipeline.mart_market_overview",
        ]

        for model_id in target_models:
            node = manifest["nodes"].get(model_id)
            assert node is not None, f"Model {model_id} not found in manifest"
            config = node.get("config", {})
            assert config.get("materialized") == "incremental", (
                f"{model_id}: expected materialized=incremental, got {config.get('materialized')}"
            )
            assert config.get("incremental_strategy") == "delete+insert", (
                f"{model_id}: expected delete+insert strategy"
            )

    def test_int_price_indicator_unique_key(self):
        if not MANIFEST_PATH.is_file():
            pytest.skip("Run dbt compile first")

        with MANIFEST_PATH.open(encoding="utf-8") as handle:
            manifest = json.load(handle)

        node = manifest["nodes"]["model.stock_pipeline.int_price_indicator"]
        unique_key = node["config"].get("unique_key", [])
        if isinstance(unique_key, str):
            unique_key = [unique_key]
        assert sorted(unique_key) == sorted(["ticker", "trading_date"]), (
            f"Expected ['ticker', 'trading_date'], got {unique_key}"
        )

    def test_mart_market_overview_unique_key(self):
        if not MANIFEST_PATH.is_file():
            pytest.skip("Run dbt compile first")

        with MANIFEST_PATH.open(encoding="utf-8") as handle:
            manifest = json.load(handle)

        node = manifest["nodes"]["model.stock_pipeline.mart_market_overview"]
        unique_key = node["config"].get("unique_key")
        assert unique_key in ("trading_date", ["trading_date"]), (
            f"Expected trading_date, got {unique_key}"
        )

    def test_dbt_schema_tests_pass_on_incremental_models(self):
        if not os.environ.get("DATABASE_URL"):
            pytest.skip("DATABASE_URL not set — skip DB tests")

        result = run_dbt(
            [
                "test",
                "--project-dir",
                str(DBT_DIR),
                "--select",
                "int_price_indicator fact_price_daily mart_stock_daily mart_market_overview",
            ]
        )
        assert result.returncode == 0, (
            f"dbt test failed:\n{result.stdout}\n{result.stderr}"
        )
