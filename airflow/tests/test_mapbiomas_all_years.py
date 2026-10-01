"""All-years selection and Airflow mapping contracts using isolated fixtures."""

from dataclasses import replace
from unittest.mock import MagicMock

import pytest

from scripts_python.mapbiomas_pipeline import MapbiomasPipelineError
from test_mapbiomas_pipeline import _service_with_sources


def test_discovery_ignores_single_year_and_finds_tifs_and_bronze(tmp_path):
    service, source, context, _ = _service_with_sources(tmp_path, {"year": 2025})
    (source / "tifs").mkdir()
    (source / "tifs" / "brazil_coverage-col11_1989.tif").write_bytes(b"fixture")
    (source / "brazil_coverage-col10_1990.tif").write_bytes(b"other collection")
    bronze = tmp_path / "bronze/mapbiomas_lulc/collection=11/version=1/year=2000"
    bronze.mkdir(parents=True)
    (bronze / "manifest.json").write_text("{}")
    assert [item["year"] for item in service.discover_datasets(context)] == [1989, 2000, 2025]


def test_discovery_includes_published_catalog_years(tmp_path, monkeypatch):
    service, _, context, _ = _service_with_sources(tmp_path)
    connection = MagicMock()
    cursor = connection.__enter__.return_value.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = [(1989,), (2025,)]
    monkeypatch.setattr("scripts_python.mapbiomas_pipeline.psycopg2.connect", lambda *args: connection)
    service.config = replace(service.config, project_db_url="mock")
    assert [item["year"] for item in service.discover_datasets(context)] == [1989, 2025]
    assert "publication_status='PUBLISHED'" in cursor.execute.call_args.args[0]


def test_empty_discovery_fails_instead_of_skipping_successfully(tmp_path):
    service, source, context, dataset = _service_with_sources(tmp_path)
    (source / dataset.source_raster_name).unlink()
    with pytest.raises(MapbiomasPipelineError, match="No available"):
        service.discover_datasets(context)


def test_tifs_source_replay_checks_bytes_and_bronze_survives_raw_removal(tmp_path):
    service, source, context, dataset = _service_with_sources(tmp_path)
    (source / "tifs").mkdir()
    raster = source / "tifs" / dataset.source_raster_name
    (source / dataset.source_raster_name).rename(raster)
    assert service.bootstrap_bronze(context)["mapbiomas"]["status"] == "published"
    assert service.bootstrap_bronze(context)["mapbiomas"]["status"] == "replayed"
    raster.write_bytes(b"changed")
    with pytest.raises(MapbiomasPipelineError, match="differs from configured source"):
        service.bootstrap_bronze(context)
    raster.unlink()
    assert service.bootstrap_bronze(context)["mapbiomas"]["status"] == "replayed"


def test_dag_maps_annual_stages_and_waits_for_all_years():
    from airflow.models.mappedoperator import MappedOperator
    from dag_mapbiomas import dag

    assert len(dag.tasks) == 14
    assert dag.max_active_runs == 1
    for task in dag.tasks:
        assert task.trigger_rule == "all_success"
        if task.task_id in {"discover_years", "publish_validation_reference", "build_aoi_snapshot", "seed_legend_postgres"}:
            assert not isinstance(task, MappedOperator)
        else:
            assert isinstance(task, MappedOperator)
            assert "discover_years" in task.upstream_task_ids
            assert task.max_active_tis_per_dag == 1
    assert [task.task_id for task in dag.leaves] == ["load_area_statistics_postgres"]


def test_shared_stage_uses_first_available_year_and_preserves_import(monkeypatch):
    from datetime import datetime
    from types import SimpleNamespace
    import dag_mapbiomas

    service = MagicMock()
    monkeypatch.setattr(dag_mapbiomas, "MapbiomasPipelineService", lambda: service)
    ti = MagicMock()
    ti.xcom_pull.return_value = [{"dataset_conf": {"collection": "11", "version": "1", "year": 1989}}]
    dag_mapbiomas._run_stage("seed_legend_postgres", dag=dag_mapbiomas.dag,
        dag_run=SimpleNamespace(conf={"year": 2025, "import_id": "upload"}),
        logical_date=datetime(2026, 9, 13), run_id="test", ti=ti)
    context = service.seed_legend_postgres.call_args.args[0]
    assert context.conf["year"] == 1989
    assert context.conf["import_id"] == "upload"
