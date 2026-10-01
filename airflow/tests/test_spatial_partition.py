"""Regression tests for exclusive UC, ZA and Buffer de Abrangência thematic partitions."""

from __future__ import annotations

from datetime import date

import geopandas as gpd
import pytest
from shapely.geometry import box

from scripts_python.mapbiomas_alerta_pipeline import MapbiomasAlertaPipelineService
from scripts_python.prodes_pipeline import ProdesPipelineService


def test_mapbiomas_alerta_serializes_dates_before_gpkg_write(tmp_path) -> None:
    """Fiona must never receive Python date objects from alert source fields."""
    service = MapbiomasAlertaPipelineService()
    path = tmp_path / "alerts.gpkg"
    frame = gpd.GeoDataFrame(
        {"dt_deteccao": [date(2026, 9, 13)], "dt_bronze": [date(2026, 9, 13)]},
        geometry=[box(0, 0, 1, 1)],
        crs="EPSG:4674",
    )

    assert service._append_gpkg_layer(frame, path, "alerts", False)
    restored = gpd.read_file(path, layer="alerts")
    assert restored.loc[0, "dt_deteccao"] == "2026-09-13"
    assert restored.loc[0, "dt_bronze"] == "2026-09-13"


@pytest.fixture
def reference_layers() -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Build a UC and a raw Buffer de Abrangência buffer that contains the UC."""
    uc = gpd.GeoDataFrame({"id_uc": [1]}, geometry=[box(0, 0, 2, 2)], crs="EPSG:4674")
    za = gpd.GeoDataFrame({"id_za_oficial": [], "id_uc": []}, geometry=[], crs="EPSG:4674")
    buffer = gpd.GeoDataFrame({"id_buffer_abrangencia": [10], "id_uc": [1]}, geometry=[box(0, 0, 5, 2)], crs="EPSG:4674")
    return uc, za, buffer


@pytest.mark.parametrize(
    ("service_class", "source_identifier"),
    [
        (ProdesPipelineService, "source_row_id"),
        (MapbiomasAlertaPipelineService, "alert_row_id"),
    ],
)
def test_crossing_feature_is_partitioned_between_uc_and_buffer(
    reference_layers: tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame],
    service_class: type,
    source_identifier: str,
) -> None:
    """Split a source feature into non-overlapping UC and exclusive-buffer parts."""
    uc, za, buffer = reference_layers
    service = service_class()
    source = gpd.GeoDataFrame(
        {source_identifier: [1]}, geometry=[box(-1, 0, 6, 2)], crs="EPSG:4674"
    )

    exclusive_buffer = service._exclusive_zone_geometries(buffer, uc)
    result, _ = service._apply_spatial_rules(source, uc, za, exclusive_buffer, set())

    areas = {
        row.tipo_cruzamento: row.geometry.area
        for _, row in result.iterrows()
    }
    assert areas == {"UC": 4.0, "BUFFER_ABRANGENCIA": 6.0}

    uc_part = result[result["tipo_cruzamento"] == "UC"].geometry.iloc[0]
    buffer_part = result[result["tipo_cruzamento"] == "BUFFER_ABRANGENCIA"].geometry.iloc[0]
    assert uc_part.intersection(buffer_part).area == 0
