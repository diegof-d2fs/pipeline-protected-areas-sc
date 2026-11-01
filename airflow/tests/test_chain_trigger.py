"""The chain trigger follows an existing run on retry and ends a failed downstream DAG once."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from airflow.exceptions import AirflowFailException, DagRunAlreadyExists
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from scripts_python import alerting, chain_trigger
from scripts_python.chain_trigger import ChainTriggerDagRunOperator, DownstreamDagFailed


def _operator() -> ChainTriggerDagRunOperator:
    return ChainTriggerDagRunOperator(
        task_id="trigger_DAG_MAPBIOMAS",
        trigger_dag_id="DAG_MAPBIOMAS",
        trigger_run_id="chain__run__DAG_MAPBIOMAS",
        wait_for_completion=True,
        poke_interval=0,
        allowed_states=["success"],
        failed_states=["failed"],
    )


def _states(monkeypatch, *states: str) -> None:
    pending = list(states)
    monkeypatch.setattr(
        chain_trigger.DagRun, "find",
        staticmethod(lambda **_: [SimpleNamespace(state=pending.pop(0) if len(pending) > 1 else pending[0])]),
    )


def test_failed_downstream_dag_is_final_and_already_notified(monkeypatch) -> None:
    monkeypatch.setattr(TriggerDagRunOperator, "execute", lambda self, context: None)
    _states(monkeypatch, "running", "failed")

    with pytest.raises(DownstreamDagFailed) as raised:
        _operator().execute({})

    assert isinstance(raised.value, AirflowFailException)
    assert raised.value.root_cause_notified is True


def test_retry_follows_the_run_created_by_the_first_attempt(monkeypatch) -> None:
    def already_exists(self, context):
        raise DagRunAlreadyExists(
            dag_run=SimpleNamespace(dag_id=self.trigger_dag_id), execution_date=None, run_id=self.trigger_run_id
        )

    monkeypatch.setattr(TriggerDagRunOperator, "execute", already_exists)
    _states(monkeypatch, "running", "success")

    _operator().execute({})


def test_trigger_requires_a_deterministic_run_id() -> None:
    with pytest.raises(ValueError):
        ChainTriggerDagRunOperator(task_id="t", trigger_dag_id="DAG_X")


def _failure_context(exception: BaseException, map_index: int = -1) -> dict:
    ti = SimpleNamespace(dag_id="DAG_X", task_id="t", run_id="r", map_index=map_index)
    return {"task_instance": ti, "dag_run": SimpleNamespace(conf={}), "exception": exception}


def test_downstream_failure_sends_no_email(monkeypatch) -> None:
    monkeypatch.setenv("ALERT_TOPIC_ARN", "arn:aws:sns:us-east-1:1:t")
    built = []
    monkeypatch.setattr(alerting, "build_failure_message", lambda *_: built.append(1))

    alerting.notify_task_failure(_failure_context(DownstreamDagFailed("DAG_Y failed")))

    assert built == []


def test_mapped_task_is_reported_by_its_first_failed_instance_only(monkeypatch) -> None:
    monkeypatch.setenv("ALERT_TOPIC_ARN", "arn:aws:sns:us-east-1:1:t")
    built = []
    monkeypatch.setattr(alerting, "build_failure_message", lambda *_: built.append(1))
    monkeypatch.setattr(alerting, "_mapped_sibling_already_failed", lambda ti: True)

    alerting.notify_task_failure(_failure_context(RuntimeError("erro"), map_index=3))

    assert built == []
