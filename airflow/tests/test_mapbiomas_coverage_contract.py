"""Coverage contract of the MapBiomas statistics load: quarantine local failures, stop systemic ones."""

from __future__ import annotations

import json
import os
import unittest
from pathlib import Path

import pandas as pd
import psycopg2
import pytest

from scripts_python import mapbiomas_pipeline
from scripts_python.config import PipelineConfig
from scripts_python.domain_pipeline import TaskExecutionContext
from scripts_python.mapbiomas_pipeline import MapbiomasDataContractError, MapbiomasPipelineService


def _service(tmp_path: Path) -> MapbiomasPipelineService:
    return MapbiomasPipelineService(
        PipelineConfig(
            project_db_url="postgresql://unused",
            mutation_db_url="",
            medallion_bronze_path=str(tmp_path / "bronze"),
            medallion_silver_path=str(tmp_path / "silver"),
            medallion_gold_path=str(tmp_path / "gold"),
            medallion_tmp_path=str(tmp_path / "tmp"),
            mapbiomas_source_dir="",
            sc_boundary_source_dir="",
            mapbiomas_statistics_source_path="",
        )
    )


def _aois(*aois: tuple[int, float, float]) -> pd.DataFrame:
    """One statistics row per AOI: (id_uc, aoi_area_ha_geodesic, classified_area_ha)."""
    frame = pd.DataFrame(aois, columns=["id_uc", "aoi_area_ha_geodesic", "classified_area_ha"])
    frame["aoi_type"] = "UC"
    frame["coverage_ratio"] = frame["classified_area_ha"] / frame["aoi_area_ha_geodesic"]
    return frame


def test_edge_effect_of_small_aois_is_accepted_and_gross_errors_are_rejected(tmp_path: Path) -> None:
    frame = _aois(
        (1, 10_000.0, 10_400.0),  # 4% acima: dentro da razão
        (2, 4.999, 5.659),        # RPPN de 5 ha: razão 1,132, excesso 0,66 ha
        (3, 0.528, 0.557),        # RPPN de 0,5 ha: razão 1,055, excesso 0,03 ha
        (4, 1_000.0, 2_000.0),    # área contada duas vezes
        (5, 0.0, 0.5),            # área do polígono nula: razão indefinida
    )

    rejected = _service(tmp_path)._coverage_rejections(frame)

    assert rejected["id_uc"].tolist() == [4, 5]
    assert set(rejected["reason_code"]) == {"COVERAGE_OUT_OF_TOLERANCE"}


def test_systemic_rejection_loads_nothing_and_reports(tmp_path: Path, monkeypatch) -> None:
    service = _service(tmp_path)
    context = TaskExecutionContext(
        "DAG_MAPBIOMAS", "load_area_statistics_postgres", "2026-11-01T00:00:00+00:00", "manual__test",
        {"year": 2020},
    )
    statistics_dir = tmp_path / "statistics"
    statistics_dir.mkdir()
    dataset = service._dataset(context)
    _aois((1, 100.0, 300.0), (2, 100.0, 100.0)).to_parquet(statistics_dir / f"{dataset.statistics_stem}.parquet")
    snapshot_dir = tmp_path / "snapshot"
    snapshot_dir.mkdir()
    (snapshot_dir / "mapbiomas_aoi_snapshot.geojson").write_text(json.dumps({"features": []}), encoding="utf-8")
    monkeypatch.setattr(service, "_statistics_dir", lambda *_, **__: statistics_dir)
    monkeypatch.setattr(service, "_validate_gold_cog", lambda *_: {"raster_checksum_sha256": "x"})
    monkeypatch.setattr(service, "_aoi_snapshot_dir", lambda *_: snapshot_dir)
    monkeypatch.setattr(service, "_aoi_geometry_index", lambda *_: ({}, "v1"))

    def no_database(*_, **__):
        raise AssertionError("a systemic rejection must not reach the database")

    monkeypatch.setattr(mapbiomas_pipeline.psycopg2, "connect", no_database)

    with pytest.raises(MapbiomasDataContractError):
        service.load_area_statistics_postgres(context)

    reports = list((tmp_path / "quality" / "rejections" / "mapbiomas_lulc").rglob("summary.json"))
    assert len(reports) == 1
    summary = json.loads(reports[0].read_text(encoding="utf-8"))
    assert summary["result_reason"] == "SYSTEMIC_REJECTION"
    assert summary["branch"] == "year=2020"
    assert summary["rejection_counts"] == {"COVERAGE_OUT_OF_TOLERANCE": 1}


@unittest.skipUnless(os.getenv("MUTATION_TEST_DB_URL"), "MUTATION_TEST_DB_URL não configurada")
class CoverageConstraintTest(unittest.TestCase):
    """The database check is the last guard and mirrors the pipeline contract."""

    def _insert(self, cursor, aoi_area: float, classified: float) -> None:
        cursor.execute(
            """
            INSERT INTO t_clip
              (id_raster_asset, id_legend_class, id_uc, collection_code, collection_version, reference_year,
               aoi_type, aoi_geometry_version, aoi_geometry_sha256, aoi_area_ha_geodesic, pixel_count,
               class_area_ha, classified_area_ha, coverage_ratio, area_method, area_method_version,
               boundary_policy, source_checksum, run_id)
            VALUES (1, 1, 1, '11', '1', 2020, 'UC', 'v1', repeat('a', 64), %s, 1, %s, %s, %s,
                    'm', '1', 'pixel_center', repeat('b', 64), 'r');
            """,
            (aoi_area, classified, classified, classified / aoi_area),
        )

    def test_small_aoi_edge_effect_passes_and_gross_error_is_refused(self) -> None:
        with psycopg2.connect(os.environ["MUTATION_TEST_DB_URL"]) as connection:
            try:
                with connection.cursor() as cursor:
                    cursor.execute("CREATE TEMP TABLE t_clip (LIKE public.mapbiomas_clip INCLUDING DEFAULTS INCLUDING CONSTRAINTS);")
                    self._insert(cursor, 4.999, 5.659)
                    self._insert(cursor, 10_000.0, 10_400.0)
                    cursor.execute("SAVEPOINT gross;")
                    with self.assertRaises(psycopg2.errors.CheckViolation):
                        self._insert(cursor, 1_000.0, 2_000.0)
                    cursor.execute("ROLLBACK TO SAVEPOINT gross;")
            finally:
                connection.rollback()
