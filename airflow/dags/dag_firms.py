"""Daily multi-source incremental workflow for NASA FIRMS detections."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from scripts_python.domain_pipeline import TaskExecutionContext
from scripts_python.firms_pipeline import FirmsPipelineService
from scripts_python.reporting_dataset import REPORTING_PUBLISHED


def _context(stage: str, kwargs: dict, window_conf: dict | None = None) -> TaskExecutionContext:
    """Build a task context while preserving the mapped source window."""
    return TaskExecutionContext(
        dag_id=kwargs["dag"].dag_id,
        stage=stage,
        logical_date=kwargs["logical_date"].isoformat(),
        run_id=kwargs["run_id"],
        conf={**dict(kwargs["dag_run"].conf or {}), **(window_conf or {})},
    )


def _resolve(**kwargs):
    """Resolve overlapping windows for all configured operational products."""
    return FirmsPipelineService().resolve_incremental_windows(
        _context("resolve_windows", kwargs)
    )


def _process(window_conf: dict, **kwargs):
    """Process one source without masking it from aggregate availability semantics."""
    return FirmsPipelineService().process_window(
        _context("process_window", kwargs, window_conf),
        suppress_errors=True,
    )


def _summarize(results: list[dict], **kwargs):
    """Publish SUCCESS or DEGRADED and fail only when every source failed."""
    return FirmsPipelineService().summarize_incremental(
        _context("summarize", kwargs),
        results,
    )


with DAG(
    dag_id="DAG_FIRMS",
    description="Ingestao FIRMS NRT diaria, independente do ciclo cadastral.",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="30 4 * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    default_args={
        "owner": "tcc-geospatial-team",
        "retries": 0,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["firms", "incremental", "medallion", "geospatial"],
) as dag:
    start = EmptyOperator(task_id="start")
    resolve_windows = PythonOperator(task_id="resolve_windows", python_callable=_resolve)
    process_window = PythonOperator.partial(
        task_id="process_window",
        python_callable=_process,
        max_active_tis_per_dag=2,
    ).expand(op_kwargs=resolve_windows.output)
    summarize = PythonOperator(
        task_id="summarize",
        outlets=[REPORTING_PUBLISHED],
        python_callable=_summarize,
        op_kwargs={"results": process_window.output},
    )
    finish = EmptyOperator(task_id="finish")

    start >> resolve_windows >> process_window >> summarize >> finish
