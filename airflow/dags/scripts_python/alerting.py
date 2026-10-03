"""Detailed failure e-mail for every Airflow task, published through SNS.

Attached to all tasks by the cluster policy in `airflow/config/airflow_local_settings.py`, so it
fires only on the final failure (after retries). The message carries what an operator needs to
act without logging into the processing node, which may already be stopped: identification of
the run, the error and the end of the traceback, the load context and direct links to the task
log in CloudWatch, the quality reports in S3 and the reprocessing runbook.
"""

from __future__ import annotations

import json
import logging
import os
import traceback
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

LOGGER = logging.getLogger(__name__)
TRACEBACK_LINES = 40
RUNBOOK_URL = "https://github.com/diisilva/pipeline-protected-areas-sc/blob/main/docs/RUNBOOK.md"


def notify_task_failure(context: dict[str, Any]) -> None:
    """Publish the failure e-mail; never raises, so alerting cannot mask the task failure."""
    topic_arn = os.environ.get("ALERT_TOPIC_ARN")
    if not topic_arn:
        return
    try:
        subject, body = build_failure_message(context, os.environ)
        import boto3

        boto3.client("sns", region_name=os.environ.get("AWS_REGION", "us-east-1")).publish(
            TopicArn=topic_arn, Subject=subject, Message=body
        )
    except Exception:
        LOGGER.exception("could not publish failure alert")


def build_failure_message(context: dict[str, Any], env: os._Environ | dict[str, str]) -> tuple[str, str]:
    ti = context["task_instance"]
    dag_run = context.get("dag_run")
    exception = context.get("exception")
    region = env.get("AWS_REGION", "us-east-1")
    log_group = env.get("AIRFLOW_TASK_LOG_GROUP", "/pa-sc/airflow/tasks")
    lake_bucket = env.get("S3_LAKE_BUCKET", "")
    conf = dict(getattr(dag_run, "conf", None) or {})

    start = ti.start_date
    end = ti.end_date or datetime.now(timezone.utc)
    duration = f"{(end - start).total_seconds():.0f} s" if start else "n/d"
    stream_prefix = f"dag_id={ti.dag_id}/run_id={ti.run_id}/task_id={ti.task_id}"

    lines = [
        "Falha definitiva de tarefa no pipeline (após as retentativas).",
        "",
        f"DAG: {ti.dag_id}",
        f"Tarefa: {ti.task_id}" + (f" (índice mapeado {ti.map_index})" if ti.map_index >= 0 else ""),
        f"Execução: {ti.run_id}",
        f"Tentativa: {ti.try_number - 1 if ti.try_number > 1 else ti.try_number} de {ti.max_tries + 1}",
        f"Início: {start.isoformat() if start else 'n/d'}",
        f"Fim: {end.isoformat()}",
        f"Duração: {duration}",
        "",
        "Erro:",
        f"  {type(exception).__name__ if exception else 'desconhecido'}: {exception}",
        "",
        "Contexto da carga:",
        *_load_context(conf),
        "",
        "Onde investigar:",
        f"  Log da tarefa (CloudWatch): {cloudwatch_stream_url(region, log_group, stream_prefix)}",
    ]
    if lake_bucket:
        lines.append(
            f"  Relatórios de qualidade (S3): https://s3.console.aws.amazon.com/s3/buckets/{lake_bucket}"
            f"?region={region}&prefix=quality/&showversions=false"
        )
    lines += [f"  Como reprocessar: {RUNBOOK_URL}", "", "Final do traceback:", *_traceback_tail(exception)]

    subject = f"[pa-sc] Falha: {ti.dag_id}.{ti.task_id}"
    return subject[:100], "\n".join(lines)


def cloudwatch_stream_url(region: str, log_group: str, stream_prefix: str) -> str:
    """Console link to the task's log streams (the console escapes `%` as `$25`)."""

    def encode(value: str) -> str:
        return quote(value, safe="").replace("%", "$25")

    return (
        f"https://{region}.console.aws.amazon.com/cloudwatch/home?region={region}"
        f"#logsV2:log-groups/log-group/{encode(log_group)}/log-events"
        f"$3FlogStreamNameFilter$3D{encode(stream_prefix)}"
    )


def _load_context(conf: dict[str, Any]) -> list[str]:
    if not conf:
        return ["  execução agendada ou manual, sem parâmetros"]
    keys = ("import_id", "operation", "domain", "manifest_key", "source_product", "start_date", "end_date", "year")
    shown = [f"  {key}: {conf[key]}" for key in keys if key in conf]
    others = {key: value for key, value in conf.items() if key not in keys}
    if others:
        shown.append(f"  demais parâmetros: {json.dumps(others, ensure_ascii=False, default=str)[:500]}")
    return shown


def _traceback_tail(exception: BaseException | None) -> list[str]:
    if exception is None:
        return ["  (sem exceção registrada)"]
    formatted = "".join(traceback.format_exception(type(exception), exception, exception.__traceback__))
    return ["  " + line for line in formatted.rstrip().splitlines()[-TRACEBACK_LINES:]]
