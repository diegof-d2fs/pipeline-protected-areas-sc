"""Builders for standardized domain DAG definitions."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor

from scripts_python.dag_factory import build_stage_callable
from scripts_python.reporting_dataset import REPORTING_PUBLISHED

STAGES = (
    "extract",
    "validate",
    "transform",
    "load_postgres",
    "load_silver",
    "load_gold",
)


def build_domain_dag(
    dag_id: str,
    domain_name: str,
    schedule: str,
    dependencies: tuple[str, ...] = (),
    stages: tuple[str, ...] = STAGES,
    trigger_dags_for_directed_run: tuple[str, ...] = (),
) -> DAG:
    """Create a domain DAG implementing a configurable stage flow."""

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

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=f"Domain pipeline DAG for {domain_name}",
        start_date=datetime(2025, 1, 1),
        catchup=False,
        schedule=schedule,
        render_template_as_native_obj=True,
        tags=["tcc", "geospatial", domain_name],
    )

    with dag:
        previous_tasks = []

        if dependencies:
            waits = []
            for parent_dag in dependencies:
                waits.append(
                    ExternalTaskSensor(
                        task_id=f"wait_{parent_dag}",
                        external_dag_id=parent_dag,
                        external_task_id="load_gold",
                        mode="reschedule",
                        poke_interval=60,
                        timeout=60 * 60,
                    )
                )
            previous_tasks.extend(waits)

        stage_tasks = []
        for stage in stages:
            stage_tasks.append(
                PythonOperator(
                    task_id=stage,
                    python_callable=build_stage_callable(domain_name, stage),
                    outlets=[REPORTING_PUBLISHED] if stage == "load_postgres" else [],
                )
            )

        if previous_tasks:
            for wait_task in previous_tasks:
                wait_task >> stage_tasks[0]

        for current_task, next_task in zip(stage_tasks, stage_tasks[1:]):
            current_task >> next_task

        if trigger_dags_for_directed_run:
            should_continue = ShortCircuitOperator(
                task_id="is_manifest_directed_run",
                python_callable=lambda **kwargs: bool(
                    (kwargs["dag_run"].conf or {}).get("manifest_key")
                ) and (kwargs["dag_run"].conf or {}).get("operation") != "extinguish",
            )
            stage_tasks[-1] >> should_continue
            for target_dag_id in trigger_dags_for_directed_run:
                should_continue >> TriggerDagRunOperator(
                    task_id=f"trigger_{target_dag_id}",
                    trigger_dag_id=target_dag_id,
                    trigger_run_id=f"chain__{{{{ run_id }}}}__{target_dag_id}",
                    conf="{{ dag_run.conf }}",
                    execution_date="{{ logical_date }}",
                    wait_for_completion=True,
                    poke_interval=30,
                    allowed_states=["success"],
                    failed_states=["failed"],
                )

    return dag
