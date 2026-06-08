from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from ingestion.structure_data.common import listing_bronze_path, load_tickers_from_listing_bronze
from ingestion.structure_data.config import DEFAULT_PRICE_BOARD_TICKERS, IngestionConfig
from ingestion.structure_data import pipeline as structure_pipeline


@pytest.fixture
def mock_listing_parquet(tmp_path, monkeypatch):
    """Bronze listing parquet at the path ``load_tickers_from_listing_bronze`` expects."""
    root = tmp_path / "raw" / "Structure_Data"
    listing_dir = root / "listing" / "master"
    listing_dir.mkdir(parents=True)
    out_file = listing_dir / "listing.parquet"
    df = pd.DataFrame(
        [
            {"symbol": "VCB", "exchange": "HOSE", "type": "stock"},
            {"symbol": "HPG", "exchange": "HOSE", "type": "stock"},
            {"symbol": "FPT", "exchange": "HOSE", "type": "stock"},
            {"symbol": "HUT", "exchange": "HNX", "type": "stock"},
            {"symbol": "SHS", "exchange": "HNX", "type": "stock"},
            {"symbol": "AMD", "exchange": "UPCOM", "type": "stock"},
        ]
    )
    df.to_parquet(out_file, index=False)

    def _lake_root(self: IngestionConfig) -> Path:
        return root

    monkeypatch.setattr(IngestionConfig, "data_lake_root", property(_lake_root))
    return out_file


def _cfg(**kwargs) -> IngestionConfig:
    base = {
        "use_listing_as_universe": False,
        "max_tickers_per_run": 5000,
        "delay_between_categories_sec": 0,
        "delay_between_batches_sec": 0,
    }
    base.update(kwargs)
    return IngestionConfig(**base)


def test_load_tickers_no_filter(mock_listing_parquet):
    cfg = _cfg(
        listing_exchange_filter=[],
        listing_security_type_filter=[],
    )
    tickers = load_tickers_from_listing_bronze(cfg)
    assert tickers == ["AMD", "FPT", "HPG", "HUT", "SHS", "VCB"]


def test_load_tickers_exchange_filter_hose_hnx(mock_listing_parquet):
    cfg = _cfg(listing_exchange_filter=["HOSE", "HNX"])
    tickers = load_tickers_from_listing_bronze(cfg)
    assert len(tickers) == 5
    assert "AMD" not in tickers


def test_load_tickers_exchange_filter_case_insensitive(mock_listing_parquet):
    cfg = _cfg(listing_exchange_filter=["hose", "hnx"])
    tickers = load_tickers_from_listing_bronze(cfg)
    assert len(tickers) == 5
    assert "AMD" not in tickers


def test_load_tickers_file_not_found(tmp_path, monkeypatch):
    root = tmp_path / "raw" / "Structure_Data"
    root.mkdir(parents=True)

    def _lake_root(self: IngestionConfig) -> Path:
        return root

    monkeypatch.setattr(IngestionConfig, "data_lake_root", property(_lake_root))
    cfg = _cfg()
    with pytest.raises(FileNotFoundError, match="Bronze listing not found"):
        load_tickers_from_listing_bronze(cfg)


def test_load_tickers_empty_after_filter(mock_listing_parquet):
    cfg = _cfg(listing_exchange_filter=["NONEXISTENT_EXCHANGE"])
    with pytest.raises(ValueError, match="No tickers remaining after filter"):
        load_tickers_from_listing_bronze(cfg)


def test_pipeline_uses_listing_universe(mock_listing_parquet, monkeypatch):
    captured_prices: list[list[str]] = []

    def _fake_listing(cfg):
        return str(listing_bronze_path(cfg))

    def _fake_prices(cfg):
        captured_prices.append(list(cfg.tickers))
        return {}

    def _fake_company(cfg):
        return ""

    def _fake_board(cfg):
        return {"outputs": [], "batches_total": 0, "batches_ok": 0, "batches_failed": 0, "failed_batches": []}

    monkeypatch.setattr(structure_pipeline, "ingest_listing", _fake_listing)
    monkeypatch.setattr(structure_pipeline, "ingest_company_overview", _fake_company)
    monkeypatch.setattr(structure_pipeline, "ingest_prices", _fake_prices)
    monkeypatch.setattr(structure_pipeline, "_ingest_price_board_snapshot", _fake_board)
    monkeypatch.setattr(structure_pipeline, "ingest_indices", lambda cfg: {})
    monkeypatch.setattr(structure_pipeline, "write_run_metadata", lambda *a, **k: None)

    listing_dir = mock_listing_parquet.parent
    listing_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"symbol": "AAA", "exchange": "HOSE", "type": "stock"},
            {"symbol": "BBB", "exchange": "HNX", "type": "stock"},
            {"symbol": "CCC", "exchange": "UPCOM", "type": "stock"},
        ]
    ).to_parquet(mock_listing_parquet, index=False)

    cfg = _cfg(
        tickers=["VCB", "HPG"],
        use_listing_as_universe=True,
        listing_exchange_filter=[],
    )
    structure_pipeline.run_structure_ingestion_pipeline(
        cfg,
        include_indices=False,
        include_price_board=True,
    )
    assert captured_prices
    assert captured_prices[0] == ["AAA", "BBB", "CCC"]


def test_pipeline_skips_listing_universe_when_disabled(monkeypatch):
    captured_prices: list[list[str]] = []
    load_called = {"value": False}

    def _fake_load(cfg):
        load_called["value"] = True
        return ["SHOULD", "NOT", "RUN"]

    def _fake_prices(cfg):
        captured_prices.append(list(cfg.tickers))
        return {}

    monkeypatch.setattr(structure_pipeline, "load_tickers_from_listing_bronze", _fake_load)
    monkeypatch.setattr(structure_pipeline, "ingest_listing", lambda cfg: "")
    monkeypatch.setattr(structure_pipeline, "ingest_indices", lambda cfg: {})
    monkeypatch.setattr(structure_pipeline, "ingest_company_overview", lambda cfg: "")
    monkeypatch.setattr(
        structure_pipeline,
        "_ingest_price_board_snapshot",
        lambda cfg: {
            "outputs": [],
            "batches_total": 0,
            "batches_ok": 0,
            "batches_failed": 0,
            "failed_batches": [],
        },
    )
    monkeypatch.setattr(structure_pipeline, "ingest_prices", _fake_prices)
    monkeypatch.setattr(structure_pipeline, "write_run_metadata", lambda *a, **k: None)

    cfg = _cfg(tickers=["VCB", "HPG"], use_listing_as_universe=False)
    structure_pipeline.run_structure_ingestion_pipeline(cfg, include_indices=False)
    assert captured_prices == [["VCB", "HPG"]]
    assert load_called["value"] is False


def test_price_batch_continues_on_error(monkeypatch, caplog):
    import logging

    cfg = _cfg(
        tickers=["A", "B", "C", "D", "E"],
        price_batch_size=2,
        delay_between_batches_sec=0,
    )
    calls: list[list[str]] = []

    def _fake_prices(batch_cfg):
        calls.append(list(batch_cfg.tickers))
        if batch_cfg.tickers == ["A", "B"]:
            raise RuntimeError("batch 0 failed")
        return {t: [] for t in batch_cfg.tickers}

    monkeypatch.setattr(structure_pipeline, "ingest_prices", _fake_prices)

    with caplog.at_level(logging.WARNING):
        result = structure_pipeline._ingest_price_batched(cfg)

    assert result["batches_failed"] == 1
    assert result["batches_ok"] == 2
    assert len(calls) == 3
    assert any("batch 0 failed" in record.message for record in caplog.records)


def test_financial_ratio_exchange_filter(mock_listing_parquet, monkeypatch):
    captured: list[list[str]] = []

    def _fake_listing(cfg):
        return str(listing_bronze_path(cfg))

    def _fake_ratio(cfg):
        captured.append(list(cfg.tickers))
        return {}

    monkeypatch.setattr(structure_pipeline, "ingest_listing", _fake_listing)
    monkeypatch.setattr(structure_pipeline, "ingest_company_overview", lambda cfg: "")
    monkeypatch.setattr(structure_pipeline, "ingest_prices", lambda cfg: {})
    monkeypatch.setattr(structure_pipeline, "ingest_indices", lambda cfg: {})
    monkeypatch.setattr(
        structure_pipeline,
        "_ingest_price_board_snapshot",
        lambda cfg: {
            "outputs": [],
            "batches_total": 0,
            "batches_ok": 0,
            "batches_failed": 0,
            "failed_batches": [],
        },
    )
    monkeypatch.setattr(structure_pipeline, "ingest_financial_ratio", _fake_ratio)
    monkeypatch.setattr(structure_pipeline, "write_run_metadata", lambda *a, **k: None)

    pd.DataFrame(
        [
            {"symbol": "VCB", "exchange": "HOSE", "type": "stock"},
            {"symbol": "HUT", "exchange": "HNX", "type": "stock"},
            {"symbol": "AMD", "exchange": "UPCOM", "type": "stock"},
        ]
    ).to_parquet(mock_listing_parquet, index=False)

    cfg = _cfg(
        use_listing_as_universe=True,
        listing_exchange_filter=[],
        financial_ratio_exchange_filter=["HOSE", "HNX"],
    )
    structure_pipeline.run_structure_full_ingestion_pipeline(cfg)
    assert captured == [["VCB", "HUT"]]


def test_price_board_single_snapshot(monkeypatch):
    saved: dict[str, object] = {}

    def _fake_ingest(cfg_run, snapshot_at=None):
        saved["tickers"] = list(cfg_run.tickers)
        saved["snapshot_at"] = snapshot_at
        return "/tmp/PRICE_BOARD_SNAPSHOT.parquet"

    monkeypatch.setattr(structure_pipeline, "ingest_price_board", _fake_ingest)

    cfg = _cfg(
        tickers=[f"T{i:02d}" for i in range(80)],
        price_board_use_listing_universe=False,
        price_board_max_tickers=50,
        price_board_batched=False,
        max_tickers_per_run=5000,
    )
    result = structure_pipeline._ingest_price_board_snapshot(cfg)

    assert len(result["outputs"]) == 1
    assert result["batches_total"] == 1
    assert result["snapshot_at"] == saved["snapshot_at"]
    assert saved["tickers"] == list(DEFAULT_PRICE_BOARD_TICKERS)


def test_price_board_listing_universe_mode(monkeypatch):
    captured: list[list[str]] = []

    def _fake_ingest(cfg_run, snapshot_at=None):
        captured.append(list(cfg_run.tickers))
        return "/tmp/PRICE_BOARD_SNAPSHOT.parquet"

    monkeypatch.setattr(structure_pipeline, "ingest_price_board", _fake_ingest)

    tickers = [f"T{i:02d}" for i in range(120)]
    cfg = _cfg(
        tickers=tickers,
        price_board_use_listing_universe=True,
        price_board_max_tickers=80,
        price_board_batched=False,
        max_tickers_per_run=5000,
    )
    structure_pipeline._ingest_price_board_snapshot(cfg)
    assert captured == [tickers[:80]]


def test_price_board_uses_watchlist_not_listing_universe(monkeypatch):
    captured: list[list[str]] = []

    def _fake_ingest(cfg_run, snapshot_at=None):
        captured.append(list(cfg_run.tickers))
        return "/tmp/PRICE_BOARD_SNAPSHOT.parquet"

    monkeypatch.setattr(structure_pipeline, "ingest_price_board", _fake_ingest)

    cfg = _cfg(
        tickers=["ZZZ", "YYY"],
        use_listing_as_universe=True,
        price_board_use_listing_universe=False,
        price_board_tickers=["AAA", "BBB"],
    )
    structure_pipeline._ingest_price_board_snapshot(cfg)
    assert captured == [["AAA", "BBB"]]


def test_price_board_batched_merge_one_snapshot(monkeypatch):
    fetch_calls: list[list[str]] = []
    save_snapshots: list[str | None] = []

    def _fake_fetch(cfg, batch):
        fetch_calls.append(list(batch))
        return pd.DataFrame({"symbol": batch}), "KBS"

    def _fake_save(cfg, frame, snapshot_at=None, data_source=""):
        save_snapshots.append(snapshot_at)
        return f"/tmp/{snapshot_at}/PRICE_BOARD_SNAPSHOT.parquet"

    monkeypatch.setattr(structure_pipeline, "fetch_price_board_frame", _fake_fetch)
    monkeypatch.setattr(structure_pipeline, "save_price_board_frame", _fake_save)

    board_tickers = [f"T{i:02d}" for i in range(120)]
    cfg = _cfg(
        tickers=["IGNORED"],
        price_board_use_listing_universe=False,
        price_board_tickers=board_tickers,
        price_board_max_tickers=120,
        price_board_batched=True,
        price_board_batch_size=50,
        max_tickers_per_run=5000,
        delay_between_batches_sec=0,
    )
    result = structure_pipeline._ingest_price_board_snapshot(cfg)

    assert len(result["outputs"]) == 1
    assert len(fetch_calls) == 3
    assert len({s for s in save_snapshots if s}) == 1
    assert result["snapshot_at"] == save_snapshots[0]


def test_config_validation():
    with pytest.raises(ValueError, match="price_batch_size"):
        IngestionConfig(price_batch_size=0)
    with pytest.raises(ValueError, match="price_batch_size"):
        IngestionConfig(price_batch_size=1001)
    with pytest.raises(ValueError, match="price_board_max_tickers"):
        IngestionConfig(price_board_max_tickers=0)
    with pytest.raises(ValueError, match="delay_between_batches_sec"):
        IngestionConfig(delay_between_batches_sec=-1)
    cfg = IngestionConfig(price_batch_size=100)
    assert cfg.price_batch_size == 100
    assert cfg.price_board_batched is True
    assert cfg.price_board_max_tickers == 5000
    assert cfg.price_board_use_listing_universe is True
    assert cfg.price_board_tickers == list(DEFAULT_PRICE_BOARD_TICKERS)
