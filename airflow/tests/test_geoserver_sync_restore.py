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


class _Plan:
    def __init__(self, publish: bool, fingerprint: str | None = "fp-novo") -> None:
        self.publish, self.fingerprint, self.reason = publish, fingerprint, "teste"


def _wire(monkeypatch, plan: _Plan, known: str | None = "fp-antigo"):
    """Fake publisher, Variable store and side effects; returns the recorded calls."""
    calls: dict = {"order": [], "variables": {"geoserver_raster_fingerprint": known} if known else {}}

    class _Bootstrap:
        @staticmethod
        def geoserver_from_env():
            return "geoserver"

        @staticmethod
        def plan_rasters(_geoserver, known_fingerprint):
            calls["known"] = known_fingerprint
            return plan

        @staticmethod
        def publish_all(_root, *, include_rasters):
            calls["order"].append(f"publish(rasters={include_rasters})")
            return ["uc"]

    from airflow.models import Variable

    monkeypatch.setattr(dag_geoserver_sync, "_publisher", lambda: _Bootstrap)
    monkeypatch.setattr(dag_geoserver_sync, "_restore_local_gold_rasters", lambda: calls["order"].append("restore"))
    monkeypatch.setattr(dag_geoserver_sync, "_refresh_serving_gold", lambda: calls["order"].append("serving"))
    monkeypatch.setattr(Variable, "get", staticmethod(lambda key, default_var=None: calls["variables"].get(key, default_var)))
    monkeypatch.setattr(Variable, "set", staticmethod(lambda key, value: calls["variables"].__setitem__(key, value)))
    return calls


def _context(conf: dict | None = None) -> dict:
    from types import SimpleNamespace

    return {"dag_run": SimpleNamespace(conf=conf or {})}


def test_weekly_sync_skips_unchanged_rasters(monkeypatch) -> None:
    calls = _wire(monkeypatch, _Plan(publish=False, fingerprint="fp-antigo"), known="fp-antigo")

    assert dag_geoserver_sync._sync_geoserver(**_context()) == ["uc"]

    assert calls["order"] == ["publish(rasters=False)"]  # no S3 pull, no SSM on the serving node
    assert calls["variables"]["geoserver_raster_fingerprint"] == "fp-antigo"


def test_changed_rasters_restore_publish_and_record_fingerprint(monkeypatch) -> None:
    calls = _wire(monkeypatch, _Plan(publish=True, fingerprint="fp-novo"))

    dag_geoserver_sync._sync_geoserver(**_context())

    assert calls["order"] == ["restore", "serving", "publish(rasters=True)"]
    assert calls["variables"]["geoserver_raster_fingerprint"] == "fp-novo"


def test_force_raster_ignores_the_recorded_fingerprint(monkeypatch) -> None:
    calls = _wire(monkeypatch, _Plan(publish=True))

    dag_geoserver_sync._sync_geoserver(**_context({"force_raster": True}))

    assert calls["known"] is None


def test_fingerprint_is_not_recorded_when_publication_fails(monkeypatch) -> None:
    import pytest

    calls = _wire(monkeypatch, _Plan(publish=True, fingerprint="fp-novo"))

    def boom(_root, *, include_rasters):
        raise SystemExit("COG publicado não encontrado no Gold")

    monkeypatch.setattr(dag_geoserver_sync._publisher(), "publish_all", staticmethod(boom))
    with pytest.raises(SystemExit):
        dag_geoserver_sync._sync_geoserver(**_context())

    assert calls["variables"]["geoserver_raster_fingerprint"] == "fp-antigo"
