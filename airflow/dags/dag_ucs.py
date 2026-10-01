"""DAG template for UCS ingestion and standardization."""

from scripts_python.domain_dag_builder import build_domain_dag


dag = build_domain_dag(
    dag_id="DAG_UCS",
    domain_name="ucs",
    schedule="@daily",
    stages=(
        "extract",
        "validate",
        "transform",
        "load_silver",
        "load_gold",
        "load_postgres",
    ),
    trigger_dags_for_directed_run=("DAG_ZA_BUFFER",),
)
