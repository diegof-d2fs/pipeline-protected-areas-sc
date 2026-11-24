"""plan_rasters: yearly rasters are republished only when something actually changed."""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path("/opt/airflow/geoserver")))
import bootstrap_layers  # noqa: E402
from bootstrap_layers import plan_rasters, raster_fingerprint  # noqa: E402

ASSETS = [
    {"reference_year": 1985, "coverage_scope": "SC", "storage_key": "a/1985.tif", "checksum_sha256": "aa"},
    {"reference_year": 2025, "coverage_scope": "SC", "storage_key": "a/2025.tif", "checksum_sha256": "bb"},
]


class _FakeGeoServer:
    def __init__(self, stores: list[str]) -> None:
        self.stores = stores

    def request(self, method, path, *_args, **_kwargs):
        assert method == "GET" and path.endswith("/coveragestores.json")
        return 200, json.dumps({"coverageStores": {"coverageStore": [{"name": n} for n in self.stores]}})


def _with_assets(monkeypatch, assets) -> None:
    monkeypatch.setattr(bootstrap_layers, "published_raster_assets", lambda _gs: assets)


ALL_STORES = ["mapbiomas_uso_cobertura_1985", "mapbiomas_uso_cobertura_2025"]


def test_fingerprint_ignores_order_and_tracks_checksum() -> None:
    assert raster_fingerprint(ASSETS) == raster_fingerprint(list(reversed(ASSETS)))
    changed = [dict(ASSETS[0], checksum_sha256="zz"), ASSETS[1]]
    assert raster_fingerprint(changed) != raster_fingerprint(ASSETS)


def test_unchanged_assets_with_all_stores_skip_rasters(monkeypatch) -> None:
    _with_assets(monkeypatch, ASSETS)

    plan = plan_rasters(_FakeGeoServer(ALL_STORES), raster_fingerprint(ASSETS))

    assert plan.publish is False and plan.fingerprint == raster_fingerprint(ASSETS)


def test_first_sync_publishes(monkeypatch) -> None:
    _with_assets(monkeypatch, ASSETS)

    assert plan_rasters(_FakeGeoServer(ALL_STORES), None).publish is True


def test_new_year_or_new_checksum_publishes(monkeypatch) -> None:
    known = raster_fingerprint(ASSETS)
    _with_assets(monkeypatch, ASSETS + [dict(ASSETS[1], reference_year=2026, storage_key="a/2026.tif")])

    assert plan_rasters(_FakeGeoServer(ALL_STORES), known).publish is True


def test_missing_store_in_geoserver_publishes(monkeypatch) -> None:
    _with_assets(monkeypatch, ASSETS)

    plan = plan_rasters(_FakeGeoServer(ALL_STORES[:1]), raster_fingerprint(ASSETS))

    assert plan.publish is True and "divergem" in plan.reason


def test_unreadable_assets_fall_back_to_full_publication(monkeypatch) -> None:
    def fail(_gs):
        raise RuntimeError("HTTP 404")

    monkeypatch.setattr(bootstrap_layers, "published_raster_assets", fail)

    plan = plan_rasters(_FakeGeoServer([]), "qualquer")

    assert plan.publish is True and plan.fingerprint is None


def test_publish_all_without_rasters_touches_only_vectors(monkeypatch) -> None:
    refreshed: list[list[str]] = []
    monkeypatch.setenv("GEOSERVER_ADMIN_PASSWORD", "x")
    monkeypatch.setenv("REPORTING_GEOSERVER_DB_PASSWORD", "x")
    monkeypatch.setattr(bootstrap_layers.GeoServer, "exists", lambda *_: True)
    monkeypatch.setattr(bootstrap_layers, "harden_services", lambda _gs: None)
    monkeypatch.setattr(bootstrap_layers, "publish_vector_layers", lambda *_: None)
    monkeypatch.setattr(bootstrap_layers, "publish_raster_layers", lambda *_: (_ for _ in ()).throw(AssertionError))
    monkeypatch.setattr(bootstrap_layers, "refresh_caches", lambda _gs, layers: refreshed.append(layers))

    layers = bootstrap_layers.publish_all(Path("/nao-usado"), include_rasters=False)

    assert layers == [layer.name for layer in bootstrap_layers.VECTOR_LAYERS]
    assert refreshed == [layers]
