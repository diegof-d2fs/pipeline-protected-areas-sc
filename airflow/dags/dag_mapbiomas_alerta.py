"""DAG for MapBiomas Alerta clipping with UC/ZA/Buffer de Abrangência conditional routing."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from scripts_python.domain_pipeline import TaskExecutionContext
from scripts_python.mapbiomas_alerta_pipeline import MapbiomasAlertaPipelineService
from scripts_python.reporting_dataset import REPORTING_PUBLISHED


SERVICE = MapbiomasAlertaPipelineService()


def _build_callable(method_name: str):
    def _callable(**kwargs):
        context = TaskExecutionContext(
            dag_id=kwargs["dag"].dag_id,
            stage=method_name,
            logical_date=kwargs["logical_date"].isoformat(),
            run_id=kwargs["run_id"],
            conf=dict(kwargs["dag_run"].conf or {}),
        )
        handler = getattr(SERVICE, method_name)
        return handler(context)

    return _callable


default_args = {
    "owner": "tcc-geospatial-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
}


with DAG(
    dag_id="DAG_MAPBIOMAS_ALERTA",
    default_args=default_args,
    description="MapBiomas Alerta clipping pipeline with SC pre-filter and zone routing",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule="0 4 * * 1",
    max_active_runs=1,
    tags=["tcc", "geospatial", "mapbiomas_alerta"],
) as dag:
    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")

    acquire_snapshot = ShortCircuitOperator(
        task_id="acquire_snapshot", python_callable=_build_callable("acquire_snapshot")
    )
    extract = PythonOperator(task_id="extract", python_callable=_build_callable("extract"))
    validate = PythonOperator(task_id="validate", python_callable=_build_callable("validate"))
    transform_uc = PythonOperator(task_id="transform_uc", python_callable=_build_callable("transform_uc"))
    transform_zone = PythonOperator(task_id="transform_zone", python_callable=_build_callable("transform_zone"))
    transform_merge = PythonOperator(task_id="transform_merge", python_callable=_build_callable("transform_merge"))

    load_postgres = PythonOperator(
        task_id="load_postgres",
        python_callable=_build_callable("load_postgres"),
        outlets=[REPORTING_PUBLISHED],
    )
    load_silver = PythonOperator(task_id="load_silver", python_callable=_build_callable("load_silver"))
    load_gold = PythonOperator(task_id="load_gold", python_callable=_build_callable("load_gold"))

    start >> acquire_snapshot >> extract >> validate
    validate >> transform_uc >> transform_merge
    validate >> transform_zone >> transform_merge
    transform_merge >> load_silver >> load_gold >> load_postgres >> finish
