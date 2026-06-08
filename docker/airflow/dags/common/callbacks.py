"""Airflow task callbacks for stock-pipeline DAGs."""

from __future__ import annotations

import logging
from typing import Any

LOGGER = logging.getLogger(__name__)


def _format_context(context: dict[str, Any]) -> str:
    dag = context.get("dag")
    task = context.get("task")
    ti = context.get("task_instance") or context.get("ti")
    dag_id = getattr(dag, "dag_id", "?")
    task_id = getattr(task, "task_id", "?")
    run_id = getattr(ti, "run_id", context.get("run_id", "?"))
    return f"dag={dag_id} task={task_id} run_id={run_id}"


def on_failure_callback(context: dict[str, Any]) -> None:
    """Log failure details (extend later with Slack/email)."""
    prefix = _format_context(context)
    exc = context.get("exception")
    LOGGER.error("Airflow task FAILED %s exception=%s", prefix, exc)


def on_success_callback(context: dict[str, Any]) -> None:
    """Log successful task completion."""
    prefix = _format_context(context)
    LOGGER.info("Airflow task SUCCESS %s", prefix)
