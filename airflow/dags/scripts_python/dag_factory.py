"""Factory helpers to build DAG stage callables with low duplication."""

from __future__ import annotations

from typing import Callable

from airflow.exceptions import AirflowFailException

from scripts_python.domain_pipeline import DomainPipelineService, TaskExecutionContext
from scripts_python.prodes_pipeline import ProdesPipelineService
from scripts_python.ucs_pipeline import InputValidationError as UcsInputValidationError
from scripts_python.ucs_pipeline import UcsPipelineService


def build_stage_callable(domain_name: str, stage: str) -> Callable:
    """Create an Airflow task callable for a domain and ETL stage.

    Parameters
    ----------
    domain_name:
        Domain identifier used by storage paths and logging.
    stage:
        One of: extract, validate, transform, load_postgres, load_silver, load_gold.
    """

    if domain_name == "ucs":
        service = UcsPipelineService()
    elif domain_name == "prodes":
        service = ProdesPipelineService()
    else:
        service = DomainPipelineService(domain_name=domain_name)

    def _callable(**kwargs):
        logical_date = kwargs["logical_date"].isoformat()
        context = TaskExecutionContext(
            dag_id=kwargs["dag"].dag_id,
            stage=stage,
            logical_date=logical_date,
            run_id=kwargs["run_id"],
            conf=dict(kwargs["dag_run"].conf or {}),
        )
        handler = getattr(service, stage)
        try:
            return handler(context)
        except UcsInputValidationError as exc:
            # Violações autoritativas do manifesto/domínio não melhoram com retry. Falhar
            # imediatamente permite que a API publique o resultado terminal correlacionado.
            raise AirflowFailException(str(exc)) from exc

    return _callable
