"""GeoServer sync restores missing published COGs from S3 before verifying checksums."""

from __future__ import annotations

import dag_geoserver_sync


class _FakeStore:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def pull(self, names, *, key_prefix=""):
        self.calls.append((names, key_prefix))
        return {"gold": 41}


def test_restore_pulls_only_gold_mapbiomas_lulc(monkeypatch) -> None:
    store = _FakeStore()
    monkeypatch.setattr(dag_geoserver_sync.MedallionStore, "from_config", classmethod(lambda cls, _cfg: store))

    assert dag_geoserver_sync._restore_local_gold_rasters() == 41
    assert store.calls == [(("gold",), "mapbiomas_lulc/")]


def test_restore_is_a_noop_without_object_storage(monkeypatch) -> None:
    monkeypatch.setattr(dag_geoserver_sync.MedallionStore, "from_config", classmethod(lambda cls, _cfg: None))

    assert dag_geoserver_sync._restore_local_gold_rasters() == 0


def test_sync_restores_before_publishing(monkeypatch) -> None:
    order: list[str] = []
    monkeypatch.setattr(dag_geoserver_sync, "_restore_local_gold_rasters", lambda: order.append("restore") or 0)
    monkeypatch.setattr(dag_geoserver_sync, "_refresh_serving_gold", lambda: order.append("serving"))

    class _Bootstrap:
        @staticmethod
        def publish_all(_root):
            order.append("publish")
            return []

    monkeypatch.setattr(dag_geoserver_sync.importlib, "import_module", lambda _name: _Bootstrap)

    dag_geoserver_sync._sync_geoserver()

    assert order == ["restore", "serving", "publish"]
