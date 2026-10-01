"""All-available-years MapBiomas workflow, including cadastral reprocessing."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts_python.domain_pipeline import TaskExecutionContext
from scripts_python.mapbiomas_pipeline import MapbiomasPipelineService
from scripts_python.reporting_dataset import REPORTING_PUBLISHED


def _context(stage, kwargs, dataset_conf=None):
    """Preserve the import identity while selecting a discovered annual dataset."""
    return TaskExecutionContext(
        dag_id=kwargs["dag"].dag_id,
        stage=stage,
        logical_date=kwargs["logical_date"].isoformat(),
        run_id=kwargs["run_id"],
        conf={**dict(kwargs["dag_run"].conf or {}), **(dataset_conf or {})},
    )


def _discover(**kwargs):
    """Return mapping arguments for every available year, never an empty success."""
    datasets = MapbiomasPipelineService().discover_datasets(_context("discover_years", kwargs))
    return [{"dataset_conf": dataset} for dataset in datasets]


def _run_stage(stage, dataset_conf=None, **kwargs):
    """Execute an annual stage or a shared stage using the first discovered year."""
    if dataset_conf is None:
        dataset_conf = kwargs["ti"].xcom_pull(task_ids="discover_years")[0]["dataset_conf"]
    return getattr(MapbiomasPipelineService(), stage)(_context(stage, kwargs, dataset_conf))


with DAG(
    dag_id="DAG_MAPBIOMAS",
    description="Cruza UCs e entornos com todos os anos MapBiomas disponiveis e publica no PostGIS.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2), "retry_exponential_backoff": True},
    tags=["mapbiomas", "medallion", "geospatial"],
) as dag:
    discover_years = PythonOperator(task_id="discover_years", python_callable=_discover)
    # Collection references and the import snapshot are shared by all years.
    # Stage barriers and one active mapped instance avoid shared-package races.
    shared_stages = {"publish_validation_reference", "build_aoi_snapshot", "seed_legend_postgres"}
    previous = discover_years
    for stage in (
        "bootstrap_bronze", "validate_bronze", "publish_validation_reference",
        "build_silver", "validate_silver", "build_aoi_snapshot",
        "compute_area_statistics", "publish_gold_cog", "reconcile_official_statistics",
        "publish_gold_statistics", "seed_legend_postgres", "load_raster_postgres",
        "load_area_statistics_postgres",
    ):
        outlets = [REPORTING_PUBLISHED] if stage == "load_area_statistics_postgres" else []
        if stage in shared_stages:
            task = PythonOperator(task_id=stage, python_callable=_run_stage, op_args=(stage,))
        else:
            task = PythonOperator.partial(
                task_id=stage, python_callable=_run_stage, op_args=(stage,),
                max_active_tis_per_dag=1, outlets=outlets,
            ).expand(op_kwargs=discover_years.output)
        previous >> task
        previous = task
