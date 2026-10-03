"""Cluster policy: every task gets the detailed failure e-mail without touching each DAG."""

from __future__ import annotations

from scripts_python.alerting import notify_task_failure


def task_policy(task) -> None:
    if task.on_failure_callback is None:
        task.on_failure_callback = notify_task_failure
