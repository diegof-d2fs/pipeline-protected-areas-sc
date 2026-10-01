"""Atomic creation of a conservation unit and its official buffer zone."""

from scripts_python.domain_dag_builder import build_domain_dag

dag = build_domain_dag(
    dag_id="DAG_UC_ZA",
    domain_name="ucs",
    schedule=None,
    stages=("extract", "validate", "transform", "load_silver", "load_gold", "load_postgres"),
    trigger_dags_for_directed_run=("DAG_PRODES", "DAG_MAPBIOMAS_ALERTA", "DAG_MAPBIOMAS"),
)
