"""Cadastral MapBiomas tests with fabricated spatial data and isolated storage."""

import json
from dataclasses import replace
from unittest.mock import MagicMock

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import Point, box

from scripts_python.domain_pipeline import TaskExecutionContext
from scripts_python.mapbiomas_pipeline import MapbiomasPipelineError
from test_mapbiomas_pipeline import _service_with_sources


def _context(import_id="new-ucs"):
    return TaskExecutionContext("DAG_MAPBIOMAS", "test", "2026-09-13", "chain__" + import_id,
                                {"import_id": import_id, "manifest_key": "ucs/manifest.json", "domain": "uc"})


def _buffer_snapshot(tmp_path, monkeypatch, point=False):
    service, _, _, dataset = _service_with_sources(tmp_path)
    boundary_dir = service._boundary_dir()
    boundary_dir.mkdir(parents=True)
    gpd.GeoDataFrame(geometry=[box(-50, -28, -48, -26)], crs=4674).to_file(boundary_dir / "limites_SC.geojson", driver="GeoJSON")
    records = []
    for index in range(3):
        x = -49.3 + index * 0.1
        geometry = Point(x, -27.1) if point else box(x, -27.12, x + 0.02, -27.1)
        records.extend([
            {"id_uc": 101 + index, "aoi_type": "UC", "zone_id": 0, "geometry": geometry},
            {"id_uc": 101 + index, "aoi_type": "BUFFER_ABRANGENCIA", "zone_id": 201 + index, "geometry": geometry.buffer(0.02)},
        ])
    monkeypatch.setattr(service, "_directed_aoi_records", lambda context: records)
    return service, dataset


def test_import_snapshot_includes_three_ucs_and_exclusive_zones(tmp_path, monkeypatch):
    service, _ = _buffer_snapshot(tmp_path, monkeypatch)
    context = _context()
    first = service.build_aoi_snapshot(context)
    replay = service.build_aoi_snapshot(context)
    assert first["uc_count"] == 3
    assert first["counts_by_type"] == {"UC": 3, "BUFFER_ABRANGENCIA": 3}
    assert replay["status"] == "replayed"
    assert replay["checksum_sha256"] == first["checksum_sha256"]
    snapshot = gpd.read_file(service._aoi_snapshot_dir(context) / "mapbiomas_aoi_snapshot.geojson").to_crs(31982)
    for _, group in snapshot.groupby("id_uc"):
        assert group.iloc[0].geometry.intersection(group.iloc[1].geometry).area < 0.01
    assert service._aoi_snapshot_dir(context) != service._aoi_snapshot_dir(_context("second-import"))
    assert not (service._aoi_snapshot_dir(TaskExecutionContext("x", "x", "x", "x", {}))).exists()


@pytest.mark.parametrize("point", [False, True])
def test_statistics_recompute_for_new_import_and_do_not_invent_point_area(tmp_path, monkeypatch, point):
    service, dataset = _buffer_snapshot(tmp_path, monkeypatch, point=point)
    context = _context()
    service.build_aoi_snapshot(context)
    silver = service._silver_lulc_dir(dataset)
    silver.mkdir(parents=True)
    legend = [{"class_id": 3, "class_name_pt_br": "Floresta", "class_name_en": "Forest", "hex_code": "#008800"}]
    (silver / dataset.legend_name).write_text(json.dumps(legend))
    with rasterio.open(silver / dataset.silver_raster_name, "w", driver="GTiff", width=100, height=100,
                       count=1, dtype="uint8", crs=4326, transform=from_origin(-49.5, -26.9, 0.01, 0.01), nodata=0) as raster:
        raster.write(np.full((100, 100), 3, dtype="uint8"), 1)
    result = service.compute_area_statistics(context)
    assert result["record_count"] == (3 if point else 6)
    assert len(result["aois_without_selected_pixels"]) == (3 if point else 0)
    frame = pd.read_parquet(service._statistics_dir(context) / "mapbiomas_area_by_aoi_class.parquet")
    assert set(frame.id_uc) == {101, 102, 103}
    assert (frame.area_ha > 0).all()
    assert service.compute_area_statistics(context)["status"] == "replayed"
    newer = _context("another-upload")
    service.build_aoi_snapshot(newer)
    assert service.compute_area_statistics(newer)["status"] == "published"

    # Reuse annual reconciliation/COG, while publishing this import's own Gold.
    quality = tmp_path / "silver" / "mapbiomas_lulc" / "quality" / dataset.partition[0] / dataset.partition[1] / dataset.partition[2] / "area=state"
    quality.mkdir(parents=True)
    (quality / "manifest.json").write_text(json.dumps({
        "total_relative_difference_pct": 0, "non_aquatic_relative_difference_pct": 0,
        "report_checksum_sha256": "reference",
    }))
    gold = service._gold_lulc_dir(dataset)
    gold.mkdir(parents=True)
    (gold / "manifest.json").write_text(json.dumps({"raster_checksum_sha256": "cog"}))
    assert service.publish_gold_statistics(context)["record_count"] == (3 if point else 6)
    assert service.publish_gold_statistics(context)["status"] == "replayed"

    connection = MagicMock()
    cursor = connection.__enter__.return_value.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = (42,)
    cursor.fetchall.side_effect = [
        [(101, "UC-A"), (102, "UC-B"), (103, "UC-C")],
        [(101,), (102,), (103,)], [], [], [(101, 201), (102, 202), (103, 203)],
        [(3, 10)], [(3, "BUFFER_ABRANGENCIA")] if point else [(3, "UC"), (3, "BUFFER_ABRANGENCIA")],
    ]
    monkeypatch.setattr("scripts_python.mapbiomas_pipeline.psycopg2.connect", lambda *args: connection)
    monkeypatch.setattr(service, "_validate_gold_cog", lambda *args: {"raster_checksum_sha256": "cog"})
    insert = MagicMock()
    monkeypatch.setattr("scripts_python.mapbiomas_pipeline.execute_values", insert)
    service.config = replace(service.config, project_db_url="postgresql://mock-only")
    loaded = service.load_area_statistics_postgres(context)
    assert loaded["source_record_count"] == (3 if point else 6)
    rows = insert.call_args.args[2]
    assert {row[2] for row in rows} == {101, 102, 103}
    assert {row[4] for row in rows if row[8] == "BUFFER_ABRANGENCIA"} == {201, 202, 203}
    assert all(row[0] == 42 and row[7] == 2025 for row in rows)


def test_manifest_resolves_text_uc_identifiers_to_committed_ids(tmp_path, monkeypatch):
    service, _, _, _ = _service_with_sources(tmp_path)
    context = _context()
    batch_dir = tmp_path / "bronze" / "ucs"
    batch_dir.mkdir(parents=True)
    (batch_dir / "canonical.geojson").write_text(json.dumps({"features": [
        {"properties": {"uc_id": f"UC-{n}"}} for n in range(3)
    ]}))
    (batch_dir / "manifest.json").write_text(json.dumps({
        "import_id": "new-ucs", "domain": "uc", "metadata": {},
        "bronze": {"canonical_key": "ucs/canonical.geojson"},
    }))
    connection = MagicMock()
    cursor = connection.__enter__.return_value.cursor.return_value.__enter__.return_value
    polygon = json.dumps(box(-49.2, -27.2, -49.1, -27.1).__geo_interface__)
    cursor.fetchall.side_effect = [
        [(100, polygon)], [("BUFFER_ABRANGENCIA", 200, polygon)],
        [(101, polygon)], [("ZA", 201, polygon)],
        [(102, polygon)], [("BUFFER_ABRANGENCIA", 202, polygon)],
    ]
    monkeypatch.setattr("scripts_python.mapbiomas_pipeline.psycopg2.connect", lambda *args: connection)
    records = service._directed_aoi_records(context)
    assert len(records) == 6
    assert {row["id_uc"] for row in records} == {100, 101, 102}
    params = [call.args[1] for call in cursor.execute.call_args_list]
    assert params[0] == ("UC-0", None, None)
    assert params[2] == ("UC-1", None, None)
    assert params[4] == ("UC-2", None, None)


def test_chain_waits_for_mapbiomas_but_not_firms():
    from dag_za_buffer import dag

    task = dag.get_task("trigger_DAG_MAPBIOMAS")
    assert task.trigger_dag_id == "DAG_MAPBIOMAS"
    assert task.wait_for_completion is True
    assert task.failed_states == ["failed"]
    assert "is_manifest_directed_run" in task.upstream_task_ids
    assert "trigger_DAG_FIRMS" not in dag.task_ids


def test_directed_partition_requires_import_identity(tmp_path):
    service, _, _, _ = _service_with_sources(tmp_path)
    with pytest.raises(MapbiomasPipelineError, match="import_id"):
        service._aoi_snapshot_dir(TaskExecutionContext("x", "x", "x", "x", {"manifest_key": "x"}))
