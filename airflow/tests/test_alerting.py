from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from scripts_python import alerting
from scripts_python.alerting import build_failure_message, cloudwatch_stream_url, notify_task_failure


def _context(conf: dict | None = None) -> dict:
    start = datetime(2026, 10, 3, 6, 0, tzinfo=timezone.utc)
    task_instance = SimpleNamespace(
        dag_id="DAG_UCS",
        task_id="load_postgres",
        run_id="api__abc",
        map_index=-1,
        try_number=4,
        max_tries=3,
        start_date=start,
        end_date=start + timedelta(seconds=42),
    )
    try:
        raise ValueError("geometria inválida na UC 25")
    except ValueError as error:
        exception = error
    return {
        "task_instance": task_instance,
        "dag_run": SimpleNamespace(conf=conf or {}),
        "exception": exception,
    }


def test_message_identifies_run_error_context_and_links() -> None:
    env = {"AWS_REGION": "us-east-1", "S3_LAKE_BUCKET": "pa-sc-lake-1", "AIRFLOW_TASK_LOG_GROUP": "/pa-sc/airflow/tasks"}

    subject, body = build_failure_message(
        _context({"import_id": "abc", "operation": "create", "manifest_key": "ucs/import_id=abc/manifest.json"}),
        env,
    )

    assert subject == "[pa-sc] Falha: DAG_UCS.load_postgres"
    for expected in (
        "DAG: DAG_UCS",
        "Tarefa: load_postgres",
        "Execução: api__abc",
        "Tentativa: 3 de 4",
        "Duração: 42 s",
        "ValueError: geometria inválida na UC 25",
        "import_id: abc",
        "operation: create",
        "logStreamNameFilter",
        "s3.console.aws.amazon.com/s3/buckets/pa-sc-lake-1",
        "RUNBOOK.md",
        'raise ValueError("geometria inválida na UC 25")',
    ):
        assert expected in body, expected


def test_cloudwatch_link_uses_console_escaping() -> None:
    url = cloudwatch_stream_url("us-east-1", "/pa-sc/airflow/tasks", "dag_id=DAG_UCS/run_id=api__abc")

    assert "log-group/$252Fpa-sc$252Fairflow$252Ftasks/log-events" in url
    assert "$3FlogStreamNameFilter$3Ddag_id$253DDAG_UCS$252Frun_id$253Dapi__abc" in url


def test_scheduled_run_without_parameters_is_described() -> None:
    _, body = build_failure_message(_context(), {})

    assert "execução agendada ou manual, sem parâmetros" in body


def test_without_topic_nothing_is_published(monkeypatch) -> None:
    monkeypatch.delenv("ALERT_TOPIC_ARN", raising=False)
    published = []
    monkeypatch.setattr(alerting, "build_failure_message", lambda *_: published.append(1))

    notify_task_failure(_context())

    assert published == []


def test_cluster_policy_attaches_alert_to_every_task() -> None:
    from airflow.models import DagBag

    bag = DagBag(include_examples=False)

    assert not bag.import_errors
    assert all(
        task.on_failure_callback is notify_task_failure or task.on_failure_callback == notify_task_failure
        for dag in bag.dags.values()
        for task in dag.tasks
    )
