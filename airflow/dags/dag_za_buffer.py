"""DAG for ZA and Buffer de Abrangência parallel processing branches."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from scripts_python.domain_pipeline import TaskExecutionContext
from scripts_python.reporting_dataset import REPORTING_PUBLISHED
from scripts_python.za_buffer_pipeline import ZaBufferPipelineService


SERVICE = ZaBufferPipelineService()


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
    dag_id="DAG_ZA_BUFFER",
    default_args=default_args,
    description="Parallel ZA and Buffer de Abrangência processing pipeline",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule="30 6 * * 0",  # semanal, após a reconciliação de UCs
    render_template_as_native_obj=True,
    tags=["tcc", "geospatial", "za_buffer"],
) as dag:
    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")

    extract_za = PythonOperator(task_id="extract_za", python_callable=_build_callable("extract_za"))
    validate_za = PythonOperator(task_id="validate_za", python_callable=_build_callable("validate_za"))
    transform_za = PythonOperator(task_id="transform_za", python_callable=_build_callable("transform_za"))
    load_silver_za = PythonOperator(task_id="load_silver_za", python_callable=_build_callable("load_silver_za"))
    load_gold_za = PythonOperator(task_id="load_gold_za", python_callable=_build_callable("load_gold_za"))
    load_postgres_za = PythonOperator(
        task_id="load_postgres_za",
        python_callable=_build_callable("load_postgres_za"),
        outlets=[REPORTING_PUBLISHED],
    )

    extract_buffer = PythonOperator(task_id="extract_buffer", python_callable=_build_callable("extract_buffer"))
    validate_buffer = PythonOperator(task_id="validate_buffer", python_callable=_build_callable("validate_buffer"))
    transform_buffer = PythonOperator(task_id="transform_buffer", python_callable=_build_callable("transform_buffer"))
    load_silver_buffer = PythonOperator(task_id="load_silver_buffer", python_callable=_build_callable("load_silver_buffer"))
    load_gold_buffer = PythonOperator(task_id="load_gold_buffer", python_callable=_build_callable("load_gold_buffer"))
    load_postgres_buffer = PythonOperator(
        task_id="load_postgres_buffer",
        python_callable=_build_callable("load_postgres_buffer"),
        outlets=[REPORTING_PUBLISHED],
    )
    validate_zone_readiness = PythonOperator(
        task_id="validate_zone_readiness",
        python_callable=_build_callable("validate_zone_readiness"),
    )

    extract_za.set_downstream(validate_za)
    validate_za.set_downstream(transform_za)
    transform_za.set_downstream(load_silver_za)
    load_silver_za.set_downstream(load_gold_za)
    load_gold_za.set_downstream(load_postgres_za)

    extract_buffer.set_downstream(validate_buffer)
    validate_buffer.set_downstream(transform_buffer)
    transform_buffer.set_downstream(load_silver_buffer)
    load_silver_buffer.set_downstream(load_gold_buffer)
    load_gold_buffer.set_downstream(load_postgres_buffer)

    start.set_downstream(extract_za)
    start.set_downstream(extract_buffer)
    load_postgres_za.set_downstream(validate_zone_readiness)
    load_postgres_buffer.set_downstream(validate_zone_readiness)
    validate_zone_readiness.set_downstream(finish)

    directed_run = ShortCircuitOperator(
        task_id="is_manifest_directed_run",
        python_callable=lambda **kwargs: bool((kwargs["dag_run"].conf or {}).get("manifest_key")),
    )
    finish.set_downstream(directed_run)
    # Apenas pipelines temáticos efetivamente implementados participam do contrato dirigido.
    # FIRMS permanece excluído enquanto seu serviço de domínio não está implementado.
    for thematic_dag_id in ("DAG_PRODES", "DAG_MAPBIOMAS_ALERTA", "DAG_MAPBIOMAS"):
        directed_run >> TriggerDagRunOperator(
            task_id=f"trigger_{thematic_dag_id}",
            trigger_dag_id=thematic_dag_id,
            trigger_run_id=f"chain__{{{{ run_id }}}}__{thematic_dag_id}",
            conf="{{ dag_run.conf }}",
            execution_date="{{ logical_date }}",
            wait_for_completion=True,
            poke_interval=30,
            allowed_states=["success"],
            failed_states=["failed"],
        )
