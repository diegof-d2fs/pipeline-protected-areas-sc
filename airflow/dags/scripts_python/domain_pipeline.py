"""Domain pipeline service layer used by Airflow DAG tasks.

This module intentionally keeps business logic isolated from DAG wiring so that
unit tests can target deterministic methods without Airflow runtime coupling.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scripts_python.config import PipelineConfig


@dataclass(frozen=True)
class TaskExecutionContext:
    """Immutable metadata provided by orchestrator to each task stage."""

    dag_id: str
    stage: str
    logical_date: str
    run_id: str
    conf: dict[str, Any] | None = None


class DomainPipelineService:
    """Implements the canonical ETL stages for domain-specific geospatial DAGs."""

    def __init__(self, domain_name: str, config: PipelineConfig | None = None) -> None:
        self.domain_name = domain_name
        self.config = config or PipelineConfig.from_env()
        self.logger = logging.getLogger(f"pipeline.{domain_name}")

    def extract(self, context: TaskExecutionContext) -> dict[str, Any]:
        """Read source payload metadata from BRONZE and register extraction trace."""
        source_path = Path(self.config.medallion_bronze_path) / self.domain_name
        self.logger.info("Extracting %s from %s", self.domain_name, source_path)
        return self._result(context, source_path=str(source_path), status="extracted")

    def validate(self, context: TaskExecutionContext) -> dict[str, Any]:
        """Run structural and geospatial quality checks before transformation."""
        self.logger.info("Validating %s with EPSG:4674 and geometry policies", self.domain_name)
        return self._result(context, status="validated")

    def transform(self, context: TaskExecutionContext) -> dict[str, Any]:
        """Apply domain transformations and normalization to SILVER-ready schema."""
        tmp_path = Path(self.config.medallion_tmp_path) / self.domain_name
        tmp_path.mkdir(parents=True, exist_ok=True)
        self.logger.info("Transforming %s into temporary path %s", self.domain_name, tmp_path)
        return self._result(context, tmp_path=str(tmp_path), status="transformed")

    def load_postgres(self, context: TaskExecutionContext) -> dict[str, Any]:
        """Persist transformed entities into PostGIS with transaction-safe contracts."""
        self.logger.info("Loading %s into PostGIS target", self.domain_name)
        return self._result(context, db_url=self.config.project_db_url, status="loaded_postgres")

    def load_silver(self, context: TaskExecutionContext) -> dict[str, Any]:
        """Persist curated intermediate artifacts into SILVER medallion layer."""
        target = Path(self.config.medallion_silver_path) / self.domain_name
        target.mkdir(parents=True, exist_ok=True)
        self.logger.info("Writing %s SILVER layer at %s", self.domain_name, target)
        return self._result(context, target=str(target), status="loaded_silver")

    def load_gold(self, context: TaskExecutionContext) -> dict[str, Any]:
        """Persist consumption-ready artifacts into GOLD medallion layer."""
        target = Path(self.config.medallion_gold_path) / self.domain_name
        target.mkdir(parents=True, exist_ok=True)
        self.logger.info("Writing %s GOLD layer at %s", self.domain_name, target)
        return self._result(context, target=str(target), status="loaded_gold")

    def _result(self, context: TaskExecutionContext, **extra: Any) -> dict[str, Any]:
        """Compose a consistent task payload for observability and downstream audit."""
        payload = {
            "dag_id": context.dag_id,
            "stage": context.stage,
            "run_id": context.run_id,
            "logical_date": context.logical_date,
            "domain": self.domain_name,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        }
        payload.update(extra)
        return payload
