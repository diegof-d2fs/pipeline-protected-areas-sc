"""Deterministic tests for complete MapBiomas Alerta Bronze snapshots."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import fiona
import geopandas as gpd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from scripts_python.config import PipelineConfig  # noqa: E402
from scripts_python.domain_pipeline import TaskExecutionContext  # noqa: E402
from scripts_python.mapbiomas_alerta_acquisition import (  # noqa: E402
    MapbiomasAlertaAcquisitionError,
    MapbiomasAlertaAcquisitionService,
)
import scripts_python.mapbiomas_alerta_acquisition as acquisition_module  # noqa: E402
from scripts_python.mapbiomas_alerta_pipeline import MapbiomasAlertaPipelineService  # noqa: E402


def _config(tmp_path: Path) -> PipelineConfig:
    return PipelineConfig(
        project_db_url="",
        mutation_db_url="",
        medallion_bronze_path=str(tmp_path / "bronze"),
        medallion_silver_path=str(tmp_path / "silver"),
        medallion_gold_path=str(tmp_path / "gold"),
        medallion_tmp_path=str(tmp_path / "tmp"),
    )


def _context(conf: dict | None = None) -> TaskExecutionContext:
    return TaskExecutionContext(
        dag_id="DAG_MAPBIOMAS_ALERTA",
        stage="acquire_snapshot",
        logical_date="2026-09-28T04:00:00+00:00",
        run_id="test__snapshot",
        conf=conf or {},
    )


def _alert(code: int) -> dict:
    return {
        "alertCode": code,
        "publishedAt": "2026-09-24",
        "detectedAt": "2026-09-01",
        "areaHa": 1.25,
        "sources": ["SAD-MATA-ATLANTICA"],
        "crossedBiomes": ["Mata Atlântica"],
        "crossedStates": ["SANTA CATARINA"],
        "crossedCities": ["Florianópolis"],
        "deforestationClasses": ["agriculture"],
        "datum": "SIRGAS 2000, EPSG 4674",
        "geometryWkt": "POLYGON ((-48 -27, -48 -27.01, -48.01 -27.01, -48 -27))",
        "imageAcquiredBeforeAt": "2026-08-15",
        "imageAcquiredAfterAt": "2026-09-01",
        "republished": False,
        "publicationCycles": 1,
    }


class FakeClient:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
        self.calls = 0
        self.max_published_at = "2026-09-24"
        self.seen_start_dates: list[str] = []
        self.seen_end_dates: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return None

    def query(self, _document: str, variables: dict | None = None) -> dict:
        if variables is None:
            return {"alertDateRange": {"maxPublishedAt": self.max_published_at}}
        self.calls += 1
        self.seen_start_dates.append(variables["startDate"])
        self.seen_end_dates.append(variables["endDate"])
        page = variables["page"]
        limit = variables["limit"]
        return {
            "alerts": {
                "metadata": {
                    "totalCount": len(self.rows),
                    "totalPages": (len(self.rows) + limit - 1) // limit,
                    "currentPage": page,
                    "limitValue": limit,
                },
                "collection": self.rows[(page - 1) * limit : page * limit],
            }
        }


def test_fetch_all_pages_in_code_order(tmp_path: Path) -> None:
    config = _config(tmp_path)
    config = PipelineConfig(**{**config.__dict__, "mapbiomas_alerta_page_size": 1})
    service = MapbiomasAlertaAcquisitionService(config)
    client = FakeClient([_alert(2), _alert(1)])
    rows = service._fetch_all(client, "2026-09-24")
    assert [row["alertCode"] for row in rows] == [1, 2]
    assert client.calls == 3  # two pages and a final count check


def test_retry_reuses_validated_pages_for_same_run(tmp_path: Path) -> None:
    config = _config(tmp_path)
    config = PipelineConfig(**{**config.__dict__, "mapbiomas_alerta_page_size": 1})
    service = MapbiomasAlertaAcquisitionService(config)
    client = FakeClient([_alert(1), _alert(2)])
    assert len(service._fetch_all(client, "2026-09-24", run_id="retry__one")) == 2
    assert client.calls == 3
    assert len(service._fetch_all(client, "2026-09-24", run_id="retry__one")) == 2
    assert client.calls == 5  # page 1 and count check are fresh; page 2 is cached
    service._discard_cache()


def test_publish_snapshot_and_manifest_without_repeating_source(tmp_path: Path) -> None:
    service = MapbiomasAlertaAcquisitionService(_config(tmp_path))
    root = Path(service.config.medallion_bronze_path) / "mapbiomas_alerta"
    root.mkdir(parents=True)
    records = [_alert(123)]
    canonical = [json.dumps(records[0], ensure_ascii=False, sort_keys=True, separators=(",", ":"))]
    service._publish(root, _context(), records, canonical, "example-sha", "2026-09-24")
    batches = [path for path in root.iterdir() if path.is_dir()]
    assert len(batches) == 1
    manifest = json.loads((batches[0] / "acquisition.json").read_text(encoding="utf-8"))
    assert manifest["record_count"] == 1
    assert manifest["source_sha256"] == "example-sha"
    assert (batches[0] / "alerts.jsonl.gz").is_file()
    with fiona.open(batches[0] / "dashboard_alerts-shapefile.shp") as dataset:
        assert len(dataset) == 1
        assert dataset.crs.to_epsg() == 4674
        assert dataset[0]["properties"]["CODEALERTA"] == 123
        assert dataset[0]["properties"]["VPRESSAO"] in (None, "")


def test_published_shapefile_satisfies_existing_extract_validate_and_date_mapping(tmp_path: Path) -> None:
    config = _config(tmp_path)
    acquisition = MapbiomasAlertaAcquisitionService(config)
    root = Path(config.medallion_bronze_path) / "mapbiomas_alerta"
    root.mkdir(parents=True)
    record = _alert(456)
    canonical = [json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"))]
    acquisition._publish(root, _context(), [record], canonical, "example-sha", "2026-09-24")

    pipeline = MapbiomasAlertaPipelineService(config)
    extracted = pipeline.extract(_context())
    pipeline.validate(_context())
    mapped = pipeline._map_to_target_schema(
        gpd.read_file(extracted["shapefile_path"]),
        extracted["dt_bronze"],
        "DAG_MAPBIOMAS_ALERTA",
        0,
    )
    assert str(mapped.iloc[0]["dt_deteccao"]) == "2026-09-01"
    assert str(mapped.iloc[0]["dt_imagem_anterior"]) == "2026-08-15"
    assert str(mapped.iloc[0]["dt_imagem_posterior"]) == "2026-09-01"


def test_directed_run_does_not_access_api(tmp_path: Path) -> None:
    service = MapbiomasAlertaAcquisitionService(_config(tmp_path))
    assert service.acquire(_context({"manifest_key": "uc/batch/manifest.json"})) is True


def test_reprocess_request_reuses_published_snapshot_without_api(tmp_path: Path, monkeypatch) -> None:
    def fail_if_called(*args, **kwargs):
        raise AssertionError("reprocessing must not call the MapBiomas Alerta API")

    monkeypatch.setattr(acquisition_module, "MapbiomasAlertaClient", fail_if_called)
    service = MapbiomasAlertaAcquisitionService(_config(tmp_path))
    assert service.acquire(_context({"reprocess_published_snapshot": True})) is True
    assert not (tmp_path / "bronze" / "mapbiomas_alerta").exists()


def test_unchanged_snapshot_does_not_create_another_bronze_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    config = PipelineConfig(
        **{**config.__dict__, "mapbiomas_alerta_email": "fake@example.org", "mapbiomas_alerta_password": "fake"}
    )
    monkeypatch.setattr(acquisition_module, "MapbiomasAlertaClient", lambda **_kwargs: FakeClient([_alert(123)]))
    service = MapbiomasAlertaAcquisitionService(config)
    assert service.acquire(_context()) is True
    assert service.acquire(_context()) is False
    root = Path(config.medallion_bronze_path) / "mapbiomas_alerta"
    assert len([path for path in root.iterdir() if path.is_dir()]) == 1


def test_next_run_fetches_only_the_incremental_window_and_merges_full_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    config = PipelineConfig(
        **{**config.__dict__, "mapbiomas_alerta_email": "fake@example.org", "mapbiomas_alerta_password": "fake"}
    )
    fake = FakeClient([_alert(1)])
    monkeypatch.setattr(acquisition_module, "MapbiomasAlertaClient", lambda **_kwargs: fake)
    service = MapbiomasAlertaAcquisitionService(config)

    assert service.acquire(_context()) is True
    assert fake.seen_start_dates[0] == acquisition_module.START_DATE

    second_alert = _alert(2)
    second_alert["publishedAt"] = "2026-09-26"
    fake.rows = [second_alert]
    fake.max_published_at = "2026-09-26"
    assert service.acquire(_context()) is True
    # The second run must only ask the API for what's new since the first run's checkpoint.
    assert fake.seen_start_dates[-1] == "2026-09-24"

    root = Path(config.medallion_bronze_path) / "mapbiomas_alerta"
    batches = sorted(path for path in root.iterdir() if path.is_dir())
    assert len(batches) == 2
    manifest = json.loads((batches[-1] / "acquisition.json").read_text(encoding="utf-8"))
    assert manifest["fetch_start_date"] == "2026-09-24"
    # Even though the fetch was incremental, the published snapshot still holds full history.
    assert manifest["record_count"] == 2
    with fiona.open(batches[-1] / "dashboard_alerts-shapefile.shp") as dataset:
        assert {feature["properties"]["CODEALERTA"] for feature in dataset} == {1, 2}


def test_invalid_wkt_is_rejected(tmp_path: Path) -> None:
    service = MapbiomasAlertaAcquisitionService(_config(tmp_path))
    record = _alert(123)
    record["geometryWkt"] = "not WKT"
    with pytest.raises(MapbiomasAlertaAcquisitionError, match="invalid geometry"):
        service._to_feature(record)


def test_explicit_recent_api_interval_preserves_history_and_records_audit(tmp_path: Path, monkeypatch) -> None:
    config = _config(tmp_path)
    fake = FakeClient([_alert(1)])
    fake.max_published_at = "2026-09-29"
    monkeypatch.setattr(acquisition_module, "MapbiomasAlertaClient", lambda **_kwargs: fake)
    service = MapbiomasAlertaAcquisitionService(config)
    assert service.acquire(_context()) is True
    new_alert = _alert(2)
    new_alert["publishedAt"] = "2026-10-02"
    fake.rows = [new_alert]
    fake.max_published_at = "2026-10-05"
    conf = {"acquisition_start_date":"2026-09-24", "acquisition_end_date":"2026-10-03"}
    assert service.acquire(_context(conf)) is True
    assert fake.seen_start_dates[-1] == "2026-09-24"
    assert fake.seen_end_dates[-1] == "2026-10-03"
    batches = sorted((Path(config.medallion_bronze_path)/"mapbiomas_alerta").iterdir())
    manifest = json.loads((batches[-1]/"acquisition.json").read_text())
    assert manifest["record_count"] == 2
    assert manifest["max_published_at"] == "2026-10-03"
    audit = json.loads((tmp_path/"quality/mapbiomas_alerta/acquisition/test__snapshot/summary.json").read_text())
    assert audit["previous_records"] == 1
    assert audit["returned_records"] == 1
    assert audit["new_or_changed_records"] == 1
    assert audit["merged_records"] == 2
    assert audit["fetch_start_date"] == "2026-09-24"
    assert audit["fetch_end_date"] == "2026-10-03"


def test_partial_api_interval_requires_complete_existing_baseline(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(acquisition_module, "MapbiomasAlertaClient", lambda **kwargs: pytest.fail("API must not be called"))
    with pytest.raises(MapbiomasAlertaAcquisitionError, match="incomplete baseline"):
        MapbiomasAlertaAcquisitionService(_config(tmp_path)).acquire(
            _context({"acquisition_start_date":"2026-09-24", "acquisition_end_date":"2026-10-03"}))