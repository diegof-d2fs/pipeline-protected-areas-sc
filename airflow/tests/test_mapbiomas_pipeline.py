"""Tests for immutable MapBiomas Bronze publication and multi-year parameterization."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

import numpy as np
import pytest
from affine import Affine

from scripts_python.config import PipelineConfig
from scripts_python.domain_pipeline import TaskExecutionContext
from scripts_python.mapbiomas_pipeline import MapbiomasDataset, MapbiomasPipelineService

DEFAULT_DATASET = MapbiomasDataset("11", "1", 2025)
RASTER_TERMINAL_CLASS_IDS = frozenset(
    {3, 4, 5, 6, 7, 9, 11, 12, 15, 20, 21, 23, 24, 25, 29, 30, 31, 32, 33,
     35, 39, 40, 41, 46, 47, 48, 49, 50, 62, 75, 77, 84, 91}
)


def _mapbiomas_source_files(dataset: MapbiomasDataset) -> tuple[str, ...]:
    """Bronze source inventory for one dataset (yearly raster plus companions)."""
    return (dataset.source_raster_name, *MapbiomasPipelineService.MAPBIOMAS_STATIC_FILES)


def _service_with_sources(
    tmp_path: Path, conf: dict | None = None
) -> tuple[MapbiomasPipelineService, Path, TaskExecutionContext, MapbiomasDataset]:
    """Create a complete small source package and its configured service."""
    dataset = MapbiomasDataset(
        str((conf or {}).get("collection", "11")),
        str((conf or {}).get("version", "1")),
        int((conf or {}).get("year", 2025)),
    )
    mapbiomas_source = tmp_path / "mapbiomas_source"
    boundary_source = tmp_path / "boundary_source"
    mapbiomas_source.mkdir()
    boundary_source.mkdir()
    for filename in _mapbiomas_source_files(dataset):
        (mapbiomas_source / filename).write_bytes(filename.encode())
    for filename in MapbiomasPipelineService.BOUNDARY_FILES:
        (boundary_source / filename).write_bytes(filename.encode())
    config = PipelineConfig(
        project_db_url="",
        mutation_db_url="",
        medallion_bronze_path=str(tmp_path / "bronze"),
        medallion_silver_path=str(tmp_path / "silver"),
        medallion_gold_path=str(tmp_path / "gold"),
        medallion_tmp_path=str(tmp_path / "tmp"),
        mapbiomas_source_dir=str(mapbiomas_source),
        sc_boundary_source_dir=str(boundary_source),
        mapbiomas_statistics_source_path="",
    )
    context = TaskExecutionContext(
        "DAG_MAPBIOMAS", "bootstrap_bronze", "2026-08-30T00:00:00+00:00", "manual__test", conf or {}
    )
    return MapbiomasPipelineService(config), mapbiomas_source, context, dataset


def test_bronze_publication_replays_without_recopying(tmp_path: Path) -> None:
    """Publish both packages once and prove that the same source replays safely."""
    service, _, context, _ = _service_with_sources(tmp_path)

    first = service.bootstrap_bronze(context)
    second = service.bootstrap_bronze(context)

    assert first["mapbiomas"]["status"] == "published"
    assert first["boundary"]["status"] == "published"
    assert second["mapbiomas"]["status"] == "replayed"
    assert second["boundary"]["status"] == "replayed"
    assert service.validate_bronze(context)["packages"][0]["domain"] == "mapbiomas_lulc"


def test_bronze_publication_rejects_missing_source_file(tmp_path: Path) -> None:
    """Reject a source package that does not contain every required artifact."""
    service, source, context, dataset = _service_with_sources(tmp_path)
    (source / dataset.source_raster_name).unlink()
    with pytest.raises(RuntimeError, match="Missing required"):
        service.bootstrap_bronze(context)


def test_bronze_publication_rejects_changed_source_on_replay(tmp_path: Path) -> None:
    """Prevent a source change from silently replacing an immutable Bronze batch."""
    service, source, context, dataset = _service_with_sources(tmp_path)
    service.bootstrap_bronze(context)
    (source / dataset.source_raster_name).write_bytes(b"changed")
    with pytest.raises(RuntimeError, match="differs from configured source"):
        service.bootstrap_bronze(context)


def test_validation_reference_replays_from_bronze_without_raw_landing(tmp_path: Path) -> None:
    """A node that only received the Bronze (no raw landing) must still publish the reference."""
    service, _, context, _ = _service_with_sources(tmp_path)
    workbook = tmp_path / "raw" / "MAPBIOMAS_BRAZIL-COL.11-BIOME_STATE.xlsx"
    workbook.parent.mkdir()
    workbook.write_bytes(b"workbook")
    service.config = PipelineConfig(**{**service.config.__dict__, "mapbiomas_statistics_source_path": str(workbook)})

    first = service.publish_validation_reference(context)
    workbook.unlink()
    second = service.publish_validation_reference(context)

    assert first["status"] == "published"
    assert second["status"] == "replayed"


def test_dataset_conf_overrides_year_in_every_path(tmp_path: Path) -> None:
    """A historical backfill run selects its own Bronze partition and raster name."""
    service, _, context, dataset = _service_with_sources(tmp_path, conf={"year": 1985})

    assert dataset.source_raster_name == "brazil_coverage-col11_1985.tif"
    result = service.bootstrap_bronze(context)

    bronze_dir = service._bronze_lulc_dir(dataset)
    assert bronze_dir.as_posix().endswith("collection=11/version=1/year=1985")
    assert (bronze_dir / "brazil_coverage-col11_1985.tif").is_file()
    assert result["mapbiomas"]["status"] == "published"

    # The default 2025 dataset is untouched by the 1985 run.
    assert not service._bronze_lulc_dir(DEFAULT_DATASET).exists()


def test_config_defaults_and_conf_resolution(tmp_path: Path) -> None:
    """PipelineConfig carries the default coordinates; dag_run.conf overrides them."""
    service, _, _, _ = _service_with_sources(tmp_path)
    assert (service.config.mapbiomas_collection, service.config.mapbiomas_version, service.config.mapbiomas_year) == (
        "11",
        "1",
        2025,
    )
    context = TaskExecutionContext("DAG_MAPBIOMAS", "x", "d", "r", {"year": 1990, "collection": "12"})
    resolved = service._dataset(context)
    assert (resolved.collection, resolved.version, resolved.year) == ("12", "1", 1990)


def test_legend_seed_is_complete_and_hierarchical() -> None:
    """The versioned legend seed matches the official Collection 11 contract."""
    seed = json.loads(MapbiomasPipelineService.LEGEND_SEED_FILE.read_text(encoding="utf-8"))
    classes = seed["classes"]
    by_code = {item["class_code"]: item for item in classes}

    assert len(classes) == 41
    assert len(by_code) == 41, "duplicate class_code in seed"

    terminals = {item["class_code"] for item in classes if item["is_terminal"]}
    assert terminals == RASTER_TERMINAL_CLASS_IDS

    # Every parent reference resolves and points at a non-terminal node.
    for item in classes:
        parent = item["parent_class_code"]
        if parent is not None:
            assert parent in by_code, f"class {item['class_code']} has unknown parent {parent}"
            assert not by_code[parent]["is_terminal"]

    # The PDF misnumbers class 48; the seed fixes its parent to Lavoura Perene (36).
    assert by_code[48]["parent_class_code"] == 36
    assert by_code[46]["parent_class_code"] == 36

    colour = re.compile(r"^#[0-9A-Fa-f]{6}$")
    assert all(colour.match(item["color_hex"]) for item in classes)


def test_tile_count_partitions_every_pixel() -> None:
    """The tile grid partitions the whole raster, including partial edges."""
    assert MapbiomasPipelineService._tile_count(20444, 12615, 512) == 40 * 25
    assert MapbiomasPipelineService._tile_count(512, 512, 512) == 1
    assert MapbiomasPipelineService._tile_count(513, 1, 512) == 2


def test_raster_tiles_are_verbatim_windows_without_resampling(tmp_path: Path) -> None:
    """Tiling is a physical partition: native grid, natural edge sizes, no padding, no new values."""
    import rasterio
    from rasterio.io import MemoryFile

    values = (np.arange(15, dtype=np.uint8).reshape(3, 5) + 1) * 3  # 5 wide, 3 tall, categorical
    pixel = 0.00026949458523585647
    src_transform = Affine(pixel, 0, -50.0, 0, -pixel, -20.0)
    src_path = tmp_path / "src.tif"
    with rasterio.open(
        src_path, "w", driver="GTiff", width=5, height=3, count=1, dtype="uint8",
        crs="EPSG:4326", transform=src_transform, nodata=0,
    ) as dataset:
        dataset.write(values, 1)

    with rasterio.open(src_path) as src:
        tiles = list(MapbiomasPipelineService._iter_raster_tiles(src, tile=2))

    # 5x3 partitioned by 2 -> 3 tile columns x 2 tile rows
    assert [(r, c) for r, c, _ in tiles] == [(0, 0), (0, 1), (0, 2), (1, 0), (1, 1), (1, 2)]

    seen_values: set[int] = set()
    covered_pixels = 0
    for row, col, payload in tiles:
        with MemoryFile(payload) as memfile, memfile.open() as dataset:
            assert dataset.transform.a == pixel and dataset.transform.e == -pixel  # native resolution
            block = dataset.read(1)
            covered_pixels += block.size
            seen_values.update(int(v) for v in np.unique(block))
            # edge tiles keep their natural (smaller) size, never padded to 2x2
            assert dataset.width == (1 if col == 2 else 2)
            assert dataset.height == (1 if row == 1 else 2)

    assert covered_pixels == values.size  # exact coverage, no gaps, no overlap
    assert seen_values == set(int(v) for v in np.unique(values))  # no new classes introduced


def test_aoi_geometry_index_hashes_each_feature(tmp_path: Path) -> None:
    """The AOI geometry index keys (external_uc, aoi_type) to a stable geometry SHA-256."""
    snapshot_dir = tmp_path / "aoi_snapshot" / "version=1"
    snapshot_dir.mkdir(parents=True)
    polygon = {"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]]}
    geojson = {
        "type": "FeatureCollection",
        "features": [
            {"type": "Feature", "properties": {"id_uc": 786, "aoi_type": "UC"}, "geometry": polygon},
            {"type": "Feature", "properties": {"id_uc": 786, "aoi_type": "ZA"}, "geometry": polygon},
        ],
    }
    (snapshot_dir / "mapbiomas_aoi_snapshot.geojson").write_text(json.dumps(geojson), encoding="utf-8")
    (snapshot_dir / "manifest.json").write_text(json.dumps({"checksum_sha256": "abc123" * 10}), encoding="utf-8")

    index, version = MapbiomasPipelineService._aoi_geometry_index(snapshot_dir)

    from shapely.geometry import shape

    expected = hashlib.sha256(shape(polygon).wkb).hexdigest()
    assert index[(786, "UC")] == expected
    assert index[(786, "ZA")] == expected
    assert version.startswith("snapshot:")
