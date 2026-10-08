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


@pytest.mark.parametrize("legacy_manifest", [False, True])
def test_recross_relates_published_history_to_new_area_without_api(tmp_path: Path, legacy_manifest: bool) -> None:
    service = FirmsPipelineService(_config(tmp_path), client_factory=lambda: pytest.fail("API chamada"))
    content = (
        CSV_HEADER + "-27.10,-49.10,330,0.4,0.4,2020-03-02,0301,N20,VIIRS,h,2,290,5,D,0\n"
    ).encode()
    window = FirmsWindow("VIIRS_NOAA20_NRT", date(2020, 3, 1), date(2020, 3, 5), "backfill")
    context = _context(window.to_conf())
    from dataclasses import replace
    bronze_context = replace(context, run_id="original_acquisition")
    bronze_dir = service._partition(service.config.medallion_bronze_path, window, bronze_context)
    bronze_dir.mkdir(parents=True)
    bronze = {"checksum_sha256": service._sha256(content), "received_at": "2020-03-06T00:00:00+00:00",
              "object_key": service._relative(bronze_dir / "response.csv"), "bytes": len(content),
              "manifest_key": service._relative(bronze_dir / "manifest.json")}
    (bronze_dir / "manifest.json").write_text(json.dumps(bronze), encoding="utf-8")
    (bronze_dir / "response.csv").write_bytes(content)
    detections, _ = service._normalize(content, window, context, bronze)
    service._publish_silver(detections, window, context, bronze)
    if legacy_manifest:
        silver_dir = service._partition(service.config.medallion_silver_path, window, context)
        manifest = json.loads((silver_dir / "manifest.json").read_text())
        manifest.pop("bronze_manifest_key")
        (silver_dir / "manifest.json").write_text(json.dumps(manifest))
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


def test_recross_first_brings_the_published_silver_from_s3(tmp_path: Path) -> None:
    service = FirmsPipelineService(_config(tmp_path))
    pulled = []

    class Bucket:
        def pull(self, names, *, key_prefix=""):
            pulled.append((names, key_prefix))
            return {"silver": 0}

    service._medallion_store = lambda: Bucket()
    service.recross_published(
        TaskExecutionContext("DAG_FIRMS_RECROSS", "recross_published", "2026-10-07", "chain__z", {})
    )
    assert pulled == [(("silver",), "firms/")]


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

def test_reference_cache_rechecks_snapshot_and_detects_geometry_change(tmp_path: Path, monkeypatch) -> None:
    from collections import deque
    import scripts_python.firms_pipeline as module

    uc = [(1, box(-49.2, -27.2, -49.0, -27.0).wkb)]
    official = [(10, 1, box(-49.5, -27.5, -48.9, -26.9).wkb)]
    moved_uc = [(1, box(-48.2, -26.2, -48.0, -26.0).wkb)]
    responses = deque([uc, official, [], uc, official, [], moved_uc, official, []])
    reads = []

    class Snapshot:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def cursor(self):
            return self

        def execute(self, query):
            reads.append(query)

        def fetchall(self):
            return responses.popleft()

    monkeypatch.setattr(module.psycopg2, "connect", lambda _: Snapshot())
    service = FirmsPipelineService(_config(tmp_path))
    conversions = []
    original = service._rows_to_gdf

    def convert(rows, columns):
        conversions.append(columns)
        return original(rows, columns)

    monkeypatch.setattr(service, "_rows_to_gdf", convert)
    first = service._load_reference_layers()
    second = service._load_reference_layers()
    assert first[0].equals(second[0])
    assert len(conversions) == 3
    changed = service._load_reference_layers()
    assert not first[0].equals(changed[0])
    assert len(conversions) == 6
    assert len(reads) == 12  # Three fresh queries plus transaction setup, for every window.
    assert not responses
@pytest.mark.parametrize("status", [400, 429])
def test_client_waits_for_confirmed_shared_quota_reset(status: int) -> None:
    sleeps: list[float] = []
    session = FakeSession([
        FakeResponse(status, b"rate limited"),
        FakeResponse(200, json.dumps({"transaction_limit": 5000, "current_transactions": 5001}).encode()),
        FakeResponse(200, CSV_HEADER.encode()),
    ])
    client = FirmsAreaClient(map_key="secret", base_url="https://example.test/api/area/csv",
                             session=session, sleeper=sleeps.append)
    response = client.fetch_csv(source_product="MODIS_SP", bbox="-54,-30,-48,-25",
                                start_date="2020-03-26", day_range=5)
    assert sleeps == [610.0]
    assert len(session.calls) == 3
    assert "mapkey_status" in session.calls[1][0]
    assert "secret" not in response.sanitized_endpoint


@pytest.mark.parametrize("quota", [
    {"transaction_limit": 5000, "current_transactions": 10},
    {"error": "invalid key"},
    {"transaction_limit": 0, "current_transactions": 0},
])
def test_client_does_not_retry_http_400_without_exhausted_quota(quota: dict) -> None:
    session = FakeSession([FakeResponse(400, b"invalid request"),
                           FakeResponse(200, json.dumps(quota).encode())])
    sleeps: list[float] = []
    client = FirmsAreaClient(map_key="secret", base_url="https://example.test/api/area/csv",
                             session=session, sleeper=sleeps.append)
    with pytest.raises(FirmsClientError, match="HTTP 400"):
        client.fetch_csv(source_product="MODIS_SP", bbox="-54,-30,-48,-25",
                         start_date="2020-03-26", day_range=5)
    assert sleeps == []
    assert len(session.calls) == 2


def test_client_quota_wait_is_bounded() -> None:
    exhausted = FakeResponse(200, b'{"transaction_limit":5000,"current_transactions":5000}')
    session = FakeSession([FakeResponse(400, b"limit"), exhausted, FakeResponse(400, b"limit")])
    sleeps: list[float] = []
    client = FirmsAreaClient(map_key="secret", base_url="https://example.test/api/area/csv",
                             max_attempts=2, session=session, sleeper=sleeps.append)
    with pytest.raises(FirmsClientError, match="HTTP 400"):
        client.fetch_csv(source_product="MODIS_SP", bbox="-54,-30,-48,-25",
                         start_date="2020-03-26", day_range=5)
    assert sleeps == [610.0]
    assert len(session.calls) == 3

def test_yearly_backfill_preserves_existing_window_grid_and_recovers_failure(tmp_path: Path) -> None:
    import os
    from dataclasses import replace
    from uuid import uuid4

    import psycopg2
    from psycopg2 import sql
    from psycopg2.extensions import make_dsn

    dsn = os.getenv("MUTATION_TEST_DB_URL")
    if not dsn:
        pytest.skip("MUTATION_TEST_DB_URL is required for the planning integration test")
    schema = "firms_plan_test_" + uuid4().hex
    with psycopg2.connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        cursor.execute(sql.SQL("""
            CREATE TABLE {}.firms_backfill_window (
                source_product text NOT NULL, start_date date NOT NULL, end_date date NOT NULL,
                processing_state text NOT NULL DEFAULT 'PENDING', last_error text, updated_at timestamptz,
                UNIQUE (source_product, start_date, end_date)
            )
        """).format(sql.Identifier(schema)))
    try:
        scoped_dsn = make_dsn(dsn, options="-c search_path=" + schema)
        service = FirmsPipelineService(replace(_config(tmp_path), project_db_url=scoped_dsn))
        context = _context({"start_date": "2020-12-28", "end_date": "2021-01-06",
                            "products": ["MODIS_SP"], "batch_size": 100, "yearly_batches": True,
                            "availability": {"MODIS_SP": {"min_date": "2000-11-01", "max_date": "2026-06-30"}}})
        first = service.plan_backfill_windows(context)
        assert [(item["window_conf"]["start_date"], item["window_conf"]["end_date"])
                for item in first] == [("2020-12-28", "2021-01-01")]
        with psycopg2.connect(scoped_dsn) as connection, connection.cursor() as cursor:
            cursor.execute("UPDATE firms_backfill_window SET processing_state='FAILED' WHERE start_date='2020-12-28'")
        assert service.plan_backfill_windows(context) == first
        with psycopg2.connect(scoped_dsn) as connection, connection.cursor() as cursor:
            cursor.execute("UPDATE firms_backfill_window SET processing_state='PUBLISHED' WHERE start_date='2020-12-28'")
        second = service.plan_backfill_windows(context)
        assert [(item["window_conf"]["start_date"], item["window_conf"]["end_date"])
                for item in second] == [("2021-01-02", "2021-01-06")]
        with psycopg2.connect(scoped_dsn) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM firms_backfill_window")
            assert cursor.fetchone()[0] == 2
            cursor.execute("UPDATE firms_backfill_window SET processing_state='PUBLISHED'")
        assert service.plan_backfill_windows(context) == []
    finally:
        with psycopg2.connect(dsn) as connection, connection.cursor() as cursor:
            cursor.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))

def test_client_reads_product_availability_without_leaking_key() -> None:
    session = FakeSession([FakeResponse(200, b"data_id,min_date,max_date\nMODIS_SP,2000-11-01,2026-06-30\n")])
    client = FirmsAreaClient(map_key="secret", base_url="https://example.test/api/area/csv", session=session)
    assert client.fetch_availability() == {"MODIS_SP": {"min_date": "2000-11-01", "max_date": "2026-06-30"}}
    assert "data_availability" in session.calls[0][0]


def test_empty_csv_does_not_load_boundary_or_reference_geometries(tmp_path: Path, monkeypatch) -> None:
    service = FirmsPipelineService(_config(tmp_path))
    def forbidden():
        raise AssertionError("Empty acquisition must not load spatial reference layers")
    monkeypatch.setattr(service, "_load_state_boundary", forbidden)
    monkeypatch.setattr(service, "_load_reference_layers", forbidden)
    window = FirmsWindow("MODIS_SP", date(2020, 1, 1), date(2020, 1, 5), "backfill")
    detections, metrics = service._normalize(CSV_HEADER.encode(), window, _context(), {"checksum_sha256": "a"*64})
    assert metrics["source_records"] == 0
    assert detections.empty
    assert service._build_relations(detections).empty

def test_availability_skips_impossible_dates_and_preserves_published_history(tmp_path: Path) -> None:
    import os
    from dataclasses import replace
    from uuid import uuid4
    import psycopg2
    from psycopg2 import sql
    from psycopg2.extensions import make_dsn

    dsn = os.getenv("MUTATION_TEST_DB_URL")
    if not dsn:
        pytest.skip("MUTATION_TEST_DB_URL is required for availability planning")
    schema = "firms_availability_test_" + uuid4().hex
    with psycopg2.connect(dsn) as c, c.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        cur.execute(sql.SQL("""CREATE TABLE {}.firms_backfill_window (
            source_product text, start_date date, end_date date,
            processing_state text DEFAULT 'PENDING', last_error text, updated_at timestamptz,
            UNIQUE (source_product, start_date, end_date))""").format(sql.Identifier(schema)))
    try:
        scoped = make_dsn(dsn, options="-c search_path="+schema)
        service = FirmsPipelineService(replace(_config(tmp_path), project_db_url=scoped))
        with psycopg2.connect(scoped) as c, c.cursor() as cur:
            cur.execute("INSERT INTO firms_backfill_window VALUES ('MODIS_SP','2020-12-01','2020-12-05','PENDING',NULL,NULL),('MODIS_SP','2020-12-16','2020-12-20','PUBLISHED',NULL,NULL)")
        conf = {"start_date":"2020-12-01", "end_date":"2020-12-20", "products":["MODIS_SP"],
                "availability":{"MODIS_SP":{"min_date":"2020-12-08","max_date":"2020-12-12"}}}
        selected = service.plan_backfill_windows(_context(conf))
        assert [w["window_conf"]["start_date"] for w in selected] == ["2020-12-06", "2020-12-11"]
        with psycopg2.connect(scoped) as c, c.cursor() as cur:
            cur.execute("SELECT start_date,processing_state FROM firms_backfill_window ORDER BY start_date")
            assert cur.fetchall() == [(date(2020,12,1),'SKIPPED_UNAVAILABLE'),
                                      (date(2020,12,6),'PENDING'), (date(2020,12,11),'PENDING'),
                                      (date(2020,12,16),'PUBLISHED')]
        # A later official release makes a formerly unavailable interval eligible again.
        conf["availability"]["MODIS_SP"]["min_date"] = "2020-12-01"
        selected = service.plan_backfill_windows(_context(conf))
        assert [w["window_conf"]["start_date"] for w in selected] == ["2020-12-01", "2020-12-06", "2020-12-11"]
        # A new replay with no existing rows never seeds unavailable intervals.
        conf["products"] = ["VIIRS_NOAA20_SP"]
        conf["availability"] = {"VIIRS_NOAA20_SP":{"min_date":"2020-12-08","max_date":"2020-12-12"}}
        assert len(service.plan_backfill_windows(_context(conf))) == 2
        with psycopg2.connect(scoped) as c, c.cursor() as cur:
            cur.execute("SELECT count(*) FROM firms_backfill_window WHERE source_product='VIIRS_NOAA20_SP'")
            assert cur.fetchone()[0] == 2
    finally:
        with psycopg2.connect(dsn) as c, c.cursor() as cur:
            cur.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))

def test_http_error_diagnostics_remove_key_and_authenticated_urls() -> None:
    session = FakeSession([FakeResponse(403, b"denied secret https://example.test/secret/details")])
    client = FirmsAreaClient(map_key="secret", base_url="https://example.test/api/area/csv", session=session)
    with pytest.raises(FirmsClientError, match="HTTP 403") as error:
        client.fetch_csv(source_product="MODIS_SP", bbox="-54,-30,-48,-25", start_date="2020-01-01", day_range=1)
    assert "secret" not in str(error.value)
    assert "https://" not in str(error.value)
    assert "denied" in str(error.value)