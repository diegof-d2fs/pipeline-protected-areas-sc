"""DAG for retention cleanup in Medallion SILVER and GOLD layers."""

from __future__ import annotations

import logging
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts_python.config import PipelineConfig

LOGGER = logging.getLogger("pipeline.medallion.cleanup")

RETENTION_DAYS = 30
TEMPORAL_FORMATS = ("%Y-%m-%d-%H-%M-%S", "%Y-%m-%d-%S-%M-%H")


def _parse_batch_datetime(batch_dir: Path) -> datetime:
    for fmt in TEMPORAL_FORMATS:
        try:
            return datetime.strptime(batch_dir.name, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return datetime.fromtimestamp(batch_dir.stat().st_mtime, tz=timezone.utc)


def _cleanup_layer(layer_root: Path, cutoff: datetime) -> dict[str, int]:
    deleted_dirs = 0
    kept_dirs = 0

    if not layer_root.exists() or not layer_root.is_dir():
        return {"deleted_dirs": deleted_dirs, "kept_dirs": kept_dirs}

    for domain_dir in layer_root.iterdir():
        if not domain_dir.is_dir():
            continue

        for batch_dir in domain_dir.iterdir():
            if not batch_dir.is_dir():
                continue

            batch_dt = _parse_batch_datetime(batch_dir)
            if batch_dt < cutoff:
                shutil.rmtree(batch_dir)
                deleted_dirs += 1
                LOGGER.info("Deleted old batch directory: %s", batch_dir)
            else:
                kept_dirs += 1

    return {"deleted_dirs": deleted_dirs, "kept_dirs": kept_dirs}


def cleanup_medallion_old_batches() -> dict[str, int]:
    config = PipelineConfig.from_env()
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=RETENTION_DAYS)

    silver_root = Path(config.medallion_silver_path)
    gold_root = Path(config.medallion_gold_path)

    silver_stats = _cleanup_layer(silver_root, cutoff)
    gold_stats = _cleanup_layer(gold_root, cutoff)

    result = {
        "retention_days": RETENTION_DAYS,
        "cutoff_utc": cutoff.isoformat(),
        "silver_deleted_dirs": silver_stats["deleted_dirs"],
        "silver_kept_dirs": silver_stats["kept_dirs"],
        "gold_deleted_dirs": gold_stats["deleted_dirs"],
        "gold_kept_dirs": gold_stats["kept_dirs"],
    }

    LOGGER.info("Medallion retention cleanup finished: %s", result)
    return result


default_args = {
    "owner": "tcc-geospatial-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="DAG_CLEANUP_MEDALLION_RETENTION",
    default_args=default_args,
    description="Cleanup SILVER/GOLD folders older than 30 days",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule="0 2 * * *",
    tags=["tcc", "geospatial", "maintenance", "cleanup"],
) as dag:
    PythonOperator(
        task_id="cleanup_old_silver_gold_batches",
        python_callable=cleanup_medallion_old_batches,
    )
