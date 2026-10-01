"""Manual full-history refresh of MapBiomas Alerta, using the thematic DAG."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    dag_id="DAG_MAPBIOMAS_ALERTA_BACKFILL",
    description="Manually refresh all published MapBiomas Alerta history since 2019",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "tcc-geospatial-team", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["tcc", "geospatial", "mapbiomas_alerta", "backfill"],
) as dag:
    TriggerDagRunOperator(
        task_id="trigger_full_snapshot",
        trigger_dag_id="DAG_MAPBIOMAS_ALERTA",
        trigger_run_id="full-snapshot__{{ run_id }}",
        conf={"mode": "api_full_backfill"},
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )
