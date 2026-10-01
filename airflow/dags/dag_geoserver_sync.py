"""Publica a camada `reporting` depois de qualquer carga: S3 primeiro, GeoServer em seguida.

Agendada pelo Dataset `REPORTING_PUBLISHED`: cargas vetoriais (UC, ZA oficial, Buffer de
Abrangência, PRODES, MapBiomas Alerta, FIRMS) e rasters MapBiomas publicados disparam uma
execução; eventos simultâneos se consolidam numa só.

1. `push_medallion`: envia ao S3 o que mudou na Medallion local (sem efeito fora da AWS).
2. `sync_geoserver`: na AWS, o nó de serviço baixa do S3 o COG Gold (SSM Run Command); depois a
   publicação de `geoserver/bootstrap_layers.py`, idempotente, registra anos raster novos,
   remove anos despublicados, recalcula extensões e descarta caches de leitura e de tiles.
"""

from __future__ import annotations

import importlib
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts_python.config import PipelineConfig
from scripts_python.object_storage import MedallionStore
from scripts_python.reporting_dataset import REPORTING_PUBLISHED

GEOSERVER_DIR = Path("/opt/airflow/geoserver")
GOLD_ROOT = Path("/opt/airflow/data/gold")
# Diretório montado (somente leitura) no container do GeoServer, no nó de serviço.
SERVING_GOLD_DIR = "/srv/pa-sc/gold"


def _push_medallion() -> dict[str, int]:
    store = MedallionStore.from_config(PipelineConfig.from_env())
    return store.push() if store is not None else {}


def _refresh_serving_gold() -> None:
    """Make the serving node mirror the published COGs before GeoServer re-reads them."""
    instance_id = os.environ.get("SERVING_INSTANCE_ID")
    if not instance_id:
        return
    import boto3

    config = PipelineConfig.from_env()
    ssm = boto3.client("ssm", region_name=config.aws_region)
    command = ssm.send_command(
        InstanceIds=[instance_id],
        DocumentName="AWS-RunShellScript",
        Parameters={
            "commands": [
                f"aws s3 sync s3://{config.s3_lake_bucket}/gold/mapbiomas_lulc "
                f"{SERVING_GOLD_DIR}/mapbiomas_lulc --delete --only-show-errors"
            ]
        },
    )["Command"]
    waiter = ssm.get_waiter("command_executed")
    waiter.wait(
        CommandId=command["CommandId"],
        InstanceId=instance_id,
        WaiterConfig={"Delay": 5, "MaxAttempts": 120},
    )


def _sync_geoserver() -> list[str]:
    _refresh_serving_gold()
    if str(GEOSERVER_DIR) not in sys.path:
        sys.path.insert(0, str(GEOSERVER_DIR))
    bootstrap_layers = importlib.import_module("bootstrap_layers")
    return bootstrap_layers.publish_all(GOLD_ROOT)


with DAG(
    dag_id="DAG_GEOSERVER_SYNC",
    description="Envia a Medallion ao S3 e publica no GeoServer as camadas atualizadas em reporting.",
    start_date=datetime(2026, 1, 1),
    schedule=[REPORTING_PUBLISHED],
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 5, "retry_delay": timedelta(minutes=2), "retry_exponential_backoff": True},
    tags=["geoserver", "reporting"],
) as dag:
    push = PythonOperator(task_id="push_medallion", python_callable=_push_medallion)
    sync = PythonOperator(task_id="sync_geoserver", python_callable=_sync_geoserver)
    push >> sync
