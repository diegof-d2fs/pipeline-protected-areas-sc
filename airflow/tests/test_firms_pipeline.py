"""Contract tests for the shared FIRMS client, service and DAG topology."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import geopandas as gpd
import pytest
from airflow.models import DagBag
from scripts_python.config import PipelineConfig
from scripts_python.domain_pipeline import TaskExecutionContext
from scripts_python.firms_client import FirmsAreaClient, FirmsClientError
from scripts_python.firms_pipeline import (
    FirmsPipelineError,
    FirmsPipelineService,
    FirmsWindow,
)
from shapely.geometry import box

CSV_HEADER = (
    "latitude,longitude,bright_ti4,scan,track,acq_date,acq_time,satellite,"
    "instrument,confidence,version,bright_ti5,frp,daynight,type\n"
)


class FakeResponse:
    def __init__(self, status_code: int, content: bytes, headers: dict[str, str] | None = None):
        self.status_code = status_code
        self.content = content
        self.headers = headers or {"Content-Type": "text/csv"}


class FakeSession:
    def __init__(self, responses: list[FakeResponse | Exception]):
        self.responses = list(responses)
        self.calls: list[tuple[str, tuple[float, float]]] = []

    def get(self, url: str, timeout: tuple[float, float]):
        self.calls.append((url, timeout))
        result = self.responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def _config(tmp_path: Path) -> PipelineConfig:
    raw = tmp_path / "raw" / "ibge"
    raw.mkdir(parents=True)
    gpd.GeoDataFrame(
        [{"name": "SC", "geometry": box(-54, -30, -48, -25)}],
        crs="EPSG:4674",
    ).to_file(raw / "limites_SC.geojson", driver="GeoJSON")
    return PipelineConfig(
        project_db_url="postgresql://unused",
        mutation_db_url="",
        medallion_bronze_path=str(tmp_path / "bronze"),
        medallion_silver_path=str(tmp_path / "silver"),
        medallion_gold_path=str(tmp_path / "gold"),
        medallion_tmp_path=str(tmp_path / "tmp"),
        sc_boundary_source_dir=str(raw),
        firms_map_key="secret-value",
    )


def _context(conf: dict | None = None) -> TaskExecutionContext:
    return TaskExecutionContext(
        dag_id="DAG_FIRMS",
        stage="test",
        logical_date="2026-09-13T04:30:00+00:00",
        run_id="manual__firms_test",
        conf=conf,
    )


def test_client_retries_throttling_and_never_returns_key_in_metadata() -> None:
    sleeps: list[float] = []
    session = FakeSession(
        [
            FakeResponse(429, b"", {"Retry-After": "0", "Content-Type": "text/plain"}),
            FakeResponse(200, CSV_HEADER.encode()),
        ]
    )
    client = FirmsAreaClient(
        map_key="top-secret",
        base_url="https://example.test/api/area/csv",
        session=session,
        sleeper=sleeps.append,
    )

    response = client.fetch_csv(
        source_product="VIIRS_NOAA20_NRT",
        bbox="-54,-30,-48,-25",
        start_date="2026-09-12",
        day_range=2,
    )

    assert len(session.calls) == 2
    assert sleeps == [0.0]
    assert "top-secret" not in response.sanitized_endpoint
    assert "{MAP_KEY}" in response.sanitized_endpoint


def test_client_rejects_contract_error_without_retry() -> None:
    session = FakeSession([FakeResponse(403, b"denied", {"Content-Type": "text/plain"})])
    client = FirmsAreaClient(
        map_key="secret",
        base_url="https://example.test",
        session=session,
        sleeper=lambda _: None,
    )

    with pytest.raises(FirmsClientError, match="HTTP 403") as error:
        client.fetch_csv(
            source_product="VIIRS_NOAA20_NRT",
            bbox="-54,-30,-48,-25",
            start_date="2026-09-12",
            day_range=1,
        )

    assert "secret" not in str(error.value)
    assert len(session.calls) == 1


def test_incremental_windows_are_bounded_and_independent(tmp_path: Path) -> None:
    service = FirmsPipelineService(_config(tmp_path))
    windows = service.resolve_incremental_windows(_context())

    assert [item["window_conf"]["source_product"] for item in windows] == [
        "VIIRS_NOAA20_NRT",
        "VIIRS_NOAA20_NRT",
        "VIIRS_NOAA21_NRT",
        "VIIRS_NOAA21_NRT",
    ]
    # Sete dias até 13/09 mais um de sobreposição, em blocos de até cinco dias por produto.
    assert sorted({(item["window_conf"]["start_date"], item["window_conf"]["end_date"]) for item in windows}) == [
        ("2026-09-06", "2026-09-10"),
        ("2026-09-11", "2026-09-13"),
    ]
    assert len(windows) == 4


def test_incremental_period_follows_configuration(tmp_path: Path) -> None:
    service = FirmsPipelineService(_config(tmp_path))
    daily = service.resolve_incremental_windows(_context({"period_days": 1}))

    assert {(item["window_conf"]["start_date"], item["window_conf"]["end_date"]) for item in daily} == {
        ("2026-09-12", "2026-09-13")
    }


def test_normalization_filters_sc_and_preserves_viirs_confidence(tmp_path: Path) -> None:
    service = FirmsPipelineService(_config(tmp_path))
    content = (
        CSV_HEADER
        + "-27.10,-49.10,330,0.4,0.4,2026-09-12,0301,N20,VIIRS,h,2,290,5,D,0\n"
        + "-27.20,-49.20,320,0.4,0.4,2026-09-12,0302,N20,VIIRS,n,2,288,4,N,0\n"
        + "-20.00,-49.20,320,0.4,0.4,2026-09-12,0303,N20,VIIRS,h,2,288,4,N,0\n"
        + "invalid,-49.20,320,0.4,0.4,2026-09-12,0304,N20,VIIRS,h,2,288,4,N,0\n"
    ).encode()
    window = FirmsWindow("VIIRS_NOAA20_NRT", date(2026, 9, 12), date(2026, 9, 12), "incremental")

    frame, metrics = service._normalize(
        content,
        window,
        _context(window.to_conf()),
        {"checksum_sha256": "a" * 64},
    )

    assert len(frame) == 2
    assert frame["confidence_raw"].tolist() == ["h", "n"]
    assert frame["publish_gold"].tolist() == [True, False]
    assert metrics == {
        "source_records": 4,
        "invalid_records": 1,
        "outside_sc_records": 1,
        "confidence_filtered_records": 0,
    }


def test_modis_threshold_is_not_treated_as_viirs_category(tmp_path: Path) -> None:
    service = FirmsPipelineService(_config(tmp_path))
    header = CSV_HEADER.replace("bright_ti4", "brightness").replace("bright_ti5", "bright_t31")
    content = (
        header
        + "-27.10,-49.10,330,1,1,2020-08-01,0301,Terra,MODIS,35,6.1,290,5,D,0\n"
        + "-27.20,-49.20,330,1,1,2020-08-01,0302,Aqua,MODIS,71,6.1,290,5,N,0\n"
        + "-27.30,-49.30,330,1,1,2020-08-01,0303,Aqua,MODIS,34,6.1,290,5,N,0\n"
    ).encode()
    window = FirmsWindow("MODIS_SP", date(2020, 8, 1), date(2020, 8, 1), "backfill")

    frame, metrics = service._normalize(
        content,
        window,
        _context(window.to_conf()),
        {"checksum_sha256": "b" * 64},
    )

    assert frame["confidence_score"].tolist() == [35, 71]
    assert frame["publish_gold"].tolist() == [False, True]
    assert set(frame["confidence_scheme"]) == {"MODIS_PERCENT"}
    assert metrics["confidence_filtered_records"] == 1


def test_modis_window_can_become_empty_after_confidence_filter(tmp_path: Path) -> None:
    service = FirmsPipelineService(_config(tmp_path))
    header = CSV_HEADER.replace("bright_ti4", "brightness").replace("bright_ti5", "bright_t31")
    content = (
        header
        + "-27.10,-49.10,330,1,1,2020-02-05,0301,Terra,MODIS,20,6.1,290,5,D,0\n"
    ).encode()
    window = FirmsWindow("MODIS_SP", date(2020, 2, 5), date(2020, 2, 9), "backfill")

    frame, metrics = service._normalize(
        content,
        window,
        _context(window.to_conf()),
        {"checksum_sha256": "d" * 64},
    )

    assert frame.empty
    assert metrics["confidence_filtered_records"] == 1
    assert service._optional_int(float("nan")) is None
    assert service._optional_int(25.0) == 25


def test_spatial_relation_prefers_uc_for_same_conservation_unit(tmp_path: Path) -> None:
    service = FirmsPipelineService(_config(tmp_path))
    content = (
        CSV_HEADER
        + "-27.10,-49.10,330,0.4,0.4,2026-09-12,0301,N20,VIIRS,h,2,290,5,D,0\n"
        + "-27.40,-49.40,330,0.4,0.4,2026-09-12,0302,N20,VIIRS,h,2,290,5,D,0\n"
    ).encode()
    window = FirmsWindow("VIIRS_NOAA20_NRT", date(2026, 9, 12), date(2026, 9, 12), "incremental")
    detections, _ = service._normalize(
        content,
        window,
        _context(window.to_conf()),
        {"checksum_sha256": "c" * 64},
    )
    ucs = gpd.GeoDataFrame(
        [{"id_uc": 1, "geometry": box(-49.2, -27.2, -49.0, -27.0)}], crs="EPSG:4674"
    )
    official = gpd.GeoDataFrame(
        [{"id_za_oficial": 10, "id_uc": 1, "geometry": box(-49.5, -27.5, -49.0, -27.0)}],
        crs="EPSG:4674",
    )
    buffer = gpd.GeoDataFrame(
        columns=["id_buffer_abrangencia", "id_uc", "geometry"], geometry="geometry", crs="EPSG:4674"
    )
    service._load_reference_layers = lambda: (ucs, official, buffer)

    relations = service._build_relations(detections)

    assert relations[["id_uc", "tipo_cruzamento"]].to_dict("records") == [
        {"id_uc": 1, "tipo_cruzamento": "UC"},
        {"id_uc": 1, "tipo_cruzamento": "ZA"},
    ]
    manifest = service._publish_gold(
        relations,
        window,
        _context(window.to_conf()),
        {"checksum_sha256": "c" * 64},
    )
    assert manifest["relation_count"] == 2
    assert Path(
        service.config.medallion_gold_path,
        manifest["object_key"].split("gold/", 1)[1],
    ).is_file()


def test_reference_snapshot_requires_exactly_one_active_zone_per_uc(tmp_path: Path) -> None:
    service = FirmsPipelineService(_config(tmp_path))
    ucs = gpd.GeoDataFrame(
        [
            {"id_uc": 1, "geometry": box(-49.2, -27.2, -49.0, -27.0)},
            {"id_uc": 2, "geometry": box(-50.2, -28.2, -50.0, -28.0)},
        ],
        crs="EPSG:4674",
    )
    official = gpd.GeoDataFrame(
        [{"id_za_oficial": 10, "id_uc": 1, "geometry": box(-49.5, -27.5, -48.9, -26.9)}],
        crs="EPSG:4674",
    )
    buffer = gpd.GeoDataFrame(
        columns=["id_buffer_abrangencia", "id_uc", "geometry"], geometry="geometry", crs="EPSG:4674"
    )

    with pytest.raises(FirmsPipelineError, match="invalid UC ids: 2"):
        service._validate_reference_layers(ucs, official, buffer)

    buffer = gpd.GeoDataFrame(
        [{"id_buffer_abrangencia": 20, "id_uc": 2, "geometry": box(-50.5, -28.5, -49.9, -27.9)}],
        crs="EPSG:4674",
    )
    service._validate_reference_layers(ucs, official, buffer)


def test_daily_summary_allows_one_source_and_rejects_total_failure(tmp_path: Path) -> None:
    service = FirmsPipelineService(_config(tmp_path))
    degraded = service.summarize_incremental(
        _context(),
        [
            {"status": "SUCCESS", "source_product": "A", "source_records": 2},
            {"status": "FAILED", "source_product": "B"},
        ],
    )
    assert degraded["status"] == "DEGRADED"
    summary_path = (
        Path(service.config.medallion_gold_path).parent
        / "quality"
        / "firms"
        / "run_id=manual__firms_test"
        / "summary.json"
    )
    assert '"status": "DEGRADED"' in summary_path.read_text(encoding="utf-8")
    with pytest.raises(FirmsPipelineError, match="All configured"):
        service.summarize_incremental(
            _context(),
            [
                {"status": "FAILED", "source_product": "A"},
                {"status": "FAILED", "source_product": "B"},
            ],
        )
    assert '"status": "FAILED"' in summary_path.read_text(encoding="utf-8")


def test_recross_relates_published_history_to_new_area_without_api(tmp_path: Path) -> None:
    service = FirmsPipelineService(_config(tmp_path), client_factory=lambda: pytest.fail("API chamada"))
    content = (
        CSV_HEADER + "-27.10,-49.10,330,0.4,0.4,2020-03-02,0301,N20,VIIRS,h,2,290,5,D,0\n"
    ).encode()
    window = FirmsWindow("VIIRS_NOAA20_NRT", date(2020, 3, 1), date(2020, 3, 5), "backfill")
    context = _context(window.to_conf())
    bronze = {"checksum_sha256": "c" * 64, "received_at": "2020-03-06T00:00:00+00:00"}
    detections, _ = service._normalize(content, window, context, bronze)
    service._publish_silver(detections, window, context, bronze)
    bronze_dir = service._partition(service.config.medallion_bronze_path, window, context)
    bronze_dir.mkdir(parents=True)
    (bronze_dir / "manifest.json").write_text(json.dumps(bronze), encoding="utf-8")
    new_uc = gpd.GeoDataFrame(
        [{"id_uc": 42, "geometry": box(-49.2, -27.2, -49.0, -27.0)}], crs="EPSG:4674"
    )
    zone = gpd.GeoDataFrame(
        [{"id_buffer_abrangencia": 7, "id_uc": 42, "geometry": box(-49.5, -27.5, -48.7, -26.7)}],
        crs="EPSG:4674",
    )
    official = gpd.GeoDataFrame(
        columns=["id_za_oficial", "id_uc", "geometry"], geometry="geometry", crs="EPSG:4674"
    )
    service._load_reference_layers = lambda: (new_uc, official, zone)
    loaded: list = []
    service._load_postgres = lambda relations, *_: loaded.append(relations) or len(relations)

    summary = service.recross_published(
        TaskExecutionContext("DAG_FIRMS_RECROSS", "recross_published", "2026-10-03", "chain__x", {})
    )

    assert summary["silver_partitions"] == 1
    assert summary["inserted_relations"] == 1
    assert loaded[0][["id_uc", "tipo_cruzamento"]].to_dict("records") == [
        {"id_uc": 42, "tipo_cruzamento": "UC"}
    ]
    gold = Path(service.config.medallion_gold_path, "firms", "recross", "run_id=chain__x")
    assert list(gold.rglob("relations.geojson"))


def test_recross_without_published_history_does_nothing(tmp_path: Path) -> None:
    service = FirmsPipelineService(_config(tmp_path))
    service._load_reference_layers = lambda: pytest.fail("sem histórico, sem consulta ao banco")

    summary = service.recross_published(
        TaskExecutionContext("DAG_FIRMS_RECROSS", "recross_published", "2026-10-03", "chain__y", {})
    )

    assert summary["silver_partitions"] == 0
    assert summary["inserted_relations"] == 0


def test_cadastral_chains_trigger_firms_recross() -> None:
    dag_bag = DagBag(include_examples=False)
    assert not dag_bag.import_errors
    for dag_id in ("DAG_ZA_BUFFER", "DAG_UC_ZA"):
        assert "trigger_DAG_FIRMS_RECROSS" in dag_bag.get_dag(dag_id).task_ids
    recross = dag_bag.get_dag("DAG_FIRMS_RECROSS")
    assert recross.schedule_interval is None
    assert [task.task_id for task in recross.tasks if task.outlets] == ["recross_published"]


def test_firms_dags_are_separate_and_have_no_cadastral_sensors() -> None:
    dag_bag = DagBag(include_examples=False)
    assert not dag_bag.import_errors
    daily = dag_bag.get_dag("DAG_FIRMS")
    backfill = dag_bag.get_dag("DAG_FIRMS_BACKFILL")

    assert daily.schedule_interval == "0 6 * * 1"
    assert backfill.schedule_interval is None
    assert set(daily.task_ids) == {
        "start", "resolve_windows", "process_window", "summarize", "finish"
    }
    assert set(backfill.task_ids) == {
        "start", "plan_windows", "process_window", "summarize", "finish"
    }
    assert all("sensor" not in task.__class__.__name__.casefold() for task in daily.tasks)


def _bronze_boundary(config: PipelineConfig) -> Path:
    package = Path(config.medallion_bronze_path, "boundaries", "source=ibge", "year=2025", "area=sc")
    package.mkdir(parents=True)
    content = (Path(config.sc_boundary_source_dir) / "limites_SC.geojson").read_bytes()
    target = package / "limites_SC.geojson"
    target.write_bytes(content)
    (package / "manifest.json").write_text(json.dumps({
        "domain": "ibge_sc_boundary",
        "files": [{"name": target.name, "checksum_sha256": FirmsPipelineService._sha256(content), "byte_size": len(content)}],
    }), encoding="utf-8")
    return target


def test_firms_replays_from_bronze_without_raw_landing(tmp_path: Path) -> None:
    config = _config(tmp_path)
    _bronze_boundary(config)
    service = FirmsPipelineService(config)
    Path(config.sc_boundary_source_dir, "limites_SC.geojson").unlink()
    content = (CSV_HEADER + "-27.10,-49.10,330,1,1,2001-03-02,0301,Terra,MODIS,90,6.1,290,5,D,0\n").encode()
    window = FirmsWindow("MODIS_SP", date(2001, 3, 1), date(2001, 3, 5), "backfill")
    detections, metrics = service._normalize(content, window, _context(window.to_conf()), {"checksum_sha256": "e" * 64})
    assert len(detections) == 1
    assert metrics["source_records"] == 1


def test_firms_rejects_corrupted_bronze_even_when_raw_is_available(tmp_path: Path) -> None:
    config = _config(tmp_path)
    target = _bronze_boundary(config)
    target.write_bytes(target.read_bytes() + b"\n")
    with pytest.raises(FirmsPipelineError, match="checksum or size mismatch"):
        FirmsPipelineService(config)._load_state_boundary()


def test_firms_rejects_uncommitted_bronze_boundary(tmp_path: Path) -> None:
    config = _config(tmp_path)
    target = _bronze_boundary(config)
    (target.parent / "manifest.json").unlink()
    with pytest.raises(FirmsPipelineError, match="manifest is missing"):
        FirmsPipelineService(config)._load_state_boundary()

def test_boundary_geometry_cache_keeps_validating_bronze_bytes(tmp_path: Path, monkeypatch) -> None:
    config = _config(tmp_path)
    target = _bronze_boundary(config)
    service = FirmsPipelineService(config)
    read_file = gpd.read_file
    calls = []

    def read_once(*args, **kwargs):
        calls.append(1)
        return read_file(*args, **kwargs)

    monkeypatch.setattr(gpd, "read_file", read_once)
    first = service._load_state_boundary()
    assert service._load_state_boundary().equals(first)
    assert len(calls) == 1
    target.write_bytes(target.read_bytes() + b"\n")
    with pytest.raises(FirmsPipelineError, match="checksum or size mismatch"):
        service._load_state_boundary()


@pytest.mark.parametrize("count", [0, 1, 12, 100])
def test_backfill_groups_preserve_every_window_and_bound_parallelism(count: int, monkeypatch) -> None:
    from datetime import datetime, timedelta, timezone
    from types import SimpleNamespace
    import dag_firms_backfill as dag_module

    windows = [{"window_conf": {"source_product": "MODIS_SP", "start_date": str(date(2000, 11, 1) + timedelta(days=5 * i)), "end_date": str(date(2000, 11, 5) + timedelta(days=5 * i)), "mode": "backfill"}} for i in range(count)]
    fake = SimpleNamespace(plan_backfill_windows=lambda context: windows)
    monkeypatch.setattr(dag_module, "FirmsPipelineService", lambda: fake)
    groups = dag_module._plan(dag=SimpleNamespace(dag_id="DAG_FIRMS_BACKFILL"), logical_date=datetime(2026, 10, 3, tzinfo=timezone.utc), run_id="replay__group_test", dag_run=SimpleNamespace(conf={}))
    assert len(groups) <= 4
    assert [conf for group in groups for conf in group["window_confs"]] == [item["window_conf"] for item in windows]
    assert all(1 <= len(group["window_confs"]) <= 25 for group in groups)


def test_backfill_group_collects_failed_windows_and_supports_old_mapping(monkeypatch) -> None:
    from datetime import datetime, timezone
    from types import SimpleNamespace
    import dag_firms_backfill as dag_module

    calls = []

    def process(context, *, suppress_errors):
        assert suppress_errors
        calls.append(context.conf["source_product"])
        return {"status": "FAILED" if context.conf["source_product"] == "failed" else "SUCCESS"}

    fake = SimpleNamespace(process_window=process, summarize_backfill=lambda context, results: results)
    monkeypatch.setattr(dag_module, "FirmsPipelineService", lambda: fake)
    kwargs = dict(dag=SimpleNamespace(dag_id="DAG_FIRMS_BACKFILL"), logical_date=datetime(2026, 10, 3, tzinfo=timezone.utc), run_id="replay__group_test", dag_run=SimpleNamespace(conf={}))
    grouped = dag_module._process(window_confs=[{"source_product": "first"}, {"source_product": "failed"}, {"source_product": "last"}], **kwargs)
    old = dag_module._process(window_conf={"source_product": "old"}, **kwargs)
    assert calls == ["first", "failed", "last", "old"]
    assert dag_module._summarize([grouped, old], **kwargs) == [{"status": "SUCCESS"}, {"status": "FAILED"}, {"status": "SUCCESS"}, {"status": "SUCCESS"}]

def test_quality_retains_each_window_of_same_product_in_one_run(tmp_path: Path) -> None:
    service = FirmsPipelineService(_config(tmp_path))
    windows = [
        FirmsWindow("MODIS_SP", date(2001, 3, 1), date(2001, 3, 5), "backfill"),
        FirmsWindow("MODIS_SP", date(2001, 3, 6), date(2001, 3, 10), "backfill"),
    ]
    for window, count in zip(windows, [7, 11]):
        service._write_quality(
            _context(window.to_conf()), window, {"source_records": count},
            {"status": "SUCCESS", "silver_records": count - 1, "gold_relations": 1},
        )
    reports = [
        json.loads(path.read_text())
        for path in (Path(service.config.medallion_gold_path).parent / "quality" / "firms").rglob("summary.json")
    ]
    assert len(reports) == 2
    assert {(item["start_date"], item["end_date"], item["source_detections"]) for item in reports} == {
        ("2001-03-01", "2001-03-05", 7), ("2001-03-06", "2001-03-10", 11),
    }