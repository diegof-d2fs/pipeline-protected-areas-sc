"""Sincroniza o GeoServer depois de qualquer carga que altere a camada `reporting`.

Agendada pelo Dataset `REPORTING_PUBLISHED`: cargas vetoriais (UC, ZA oficial, Buffer de
Abrangência, PRODES, MapBiomas Alerta, FIRMS) e rasters MapBiomas publicados disparam uma
execução; eventos simultâneos se consolidam numa só. A publicação é a de
`geoserver/bootstrap_layers.py`, idempotente: registra anos raster novos, remove anos
despublicados, recalcula extensões e descarta caches de leitura e de tiles.
"""

from __future__ import annotations

import importlib
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts_python.reporting_dataset import REPORTING_PUBLISHED

GEOSERVER_DIR = Path("/opt/airflow/geoserver")
GOLD_ROOT = Path("/opt/airflow/data/gold")


def _sync_geoserver() -> list[str]:
    if str(GEOSERVER_DIR) not in sys.path:
        sys.path.insert(0, str(GEOSERVER_DIR))
    bootstrap_layers = importlib.import_module("bootstrap_layers")
    return bootstrap_layers.publish_all(GOLD_ROOT)


with DAG(
    dag_id="DAG_GEOSERVER_SYNC",
    description="Publica no GeoServer as camadas vetoriais e raster atualizadas em reporting.",
    start_date=datetime(2026, 1, 1),
    schedule=[REPORTING_PUBLISHED],
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 5, "retry_delay": timedelta(minutes=2), "retry_exponential_backoff": True},
    tags=["geoserver", "reporting"],
) as dag:
    PythonOperator(task_id="sync_geoserver", python_callable=_sync_geoserver)
