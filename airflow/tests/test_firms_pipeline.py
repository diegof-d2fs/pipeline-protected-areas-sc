"""Contract tests for the shared FIRMS client, service and DAG topology."""

from __future__ import annotations

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
