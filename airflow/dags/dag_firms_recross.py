"""Relaciona os focos FIRMS já publicados com UCs e zonas novas ou alteradas.

Disparada na cadeia cadastral, ao lado de PRODES, MapBiomas Alerta e MapBiomas: uma UC, ZA
oficial ou Buffer de Abrangência novo recebe todo o histórico de focos já carregado, a partir
da Silver, sem nova consulta à API do FIRMS. A aquisição continua nas DAGs `DAG_FIRMS`
(semanal) e `DAG_FIRMS_BACKFILL` (manual).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts_python.domain_pipeline import TaskExecutionContext
from scripts_python.firms_pipeline import FirmsPipelineService
from scripts_python.reporting_dataset import REPORTING_PUBLISHED


def _recross(**kwargs):
    context = TaskExecutionContext(
        dag_id=kwargs["dag"].dag_id,
        stage="recross_published",
        logical_date=kwargs["logical_date"].isoformat(),
        run_id=kwargs["run_id"],
        conf=dict(kwargs["dag_run"].conf or {}),
    )
    return FirmsPipelineService().recross_published(context)


with DAG(
    dag_id="DAG_FIRMS_RECROSS",
    description="Relaciona o histórico FIRMS publicado com as áreas cadastrais vigentes.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2), "retry_exponential_backoff": True},
    tags=["firms", "cadastral", "geospatial"],
) as dag:
    PythonOperator(
        task_id="recross_published",
        python_callable=_recross,
        outlets=[REPORTING_PUBLISHED],
    )
