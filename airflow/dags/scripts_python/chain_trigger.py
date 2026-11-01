"""Trigger of the next DAG in the cadastral chain (UCS -> ZA_BUFFER -> thematic DAGs).

The stock TriggerDagRunOperator has two defects for a chain that waits for completion:
- when the triggered DAG fails, the exception is retryable, and every retry tries to create
  the same run again and fails with DagRunAlreadyExists, keeping the node busy for nothing;
- each level of the chain then sends its own failure e-mail for a cause already reported by
  the task that actually failed.

This operator follows the existing run on a retry (a retry is only useful after an
infrastructure error, such as a lost worker) and ends a failed downstream DAG with a
non-retryable exception marked as already notified, so one cause yields one e-mail.
"""

from __future__ import annotations

import time

from airflow.exceptions import AirflowFailException, DagRunAlreadyExists
from airflow.models.dagrun import DagRun
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


class DownstreamDagFailed(AirflowFailException):
    """The triggered DAG failed; its own failed task already sent the alert."""

    root_cause_notified = True


class ChainTriggerDagRunOperator(TriggerDagRunOperator):
    """Trigger a DAG with a fixed run id and wait for it, without re-triggering on retry."""

    def __init__(self, **kwargs) -> None:
        if not kwargs.get("trigger_run_id"):
            raise ValueError("ChainTriggerDagRunOperator needs a deterministic trigger_run_id.")
        super().__init__(**kwargs)

    def execute(self, context):
        wait_for_completion, self.wait_for_completion = self.wait_for_completion, False
        try:
            super().execute(context)
        except DagRunAlreadyExists:
            self.log.info("Run %s of %s already exists; following it.", self.trigger_run_id, self.trigger_dag_id)
        finally:
            self.wait_for_completion = wait_for_completion
        if self.wait_for_completion:
            self._wait(str(self.trigger_run_id))

    def _wait(self, run_id: str) -> None:
        while True:
            dag_runs = DagRun.find(dag_id=self.trigger_dag_id, run_id=run_id)
            state = dag_runs[0].state if dag_runs else None
            if state in self.failed_states:
                raise DownstreamDagFailed(f"{self.trigger_dag_id} run {run_id} ended in state {state}.")
            if state in self.allowed_states:
                self.log.info("%s run %s finished with state %s.", self.trigger_dag_id, run_id, state)
                return
            self.log.info("Waiting for %s run %s (state %s).", self.trigger_dag_id, run_id, state)
            time.sleep(self.poke_interval)
