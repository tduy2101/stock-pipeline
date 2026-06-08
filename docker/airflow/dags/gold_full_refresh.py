"""Daily full gold refresh: dbt run + dbt test (19:00 ICT)."""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from common.callbacks import on_failure_callback, on_success_callback
from common.tasks import dbt_run_full, dbt_test_full

DEFAULT_ARGS = {
    "owner": "stock-pipeline",
    "retries": 0,
    "on_failure_callback": on_failure_callback,
    "on_success_callback": on_success_callback,
}

with DAG(
    dag_id="gold_full_refresh",
    description="Full dbt run and test for entire gold project",
    schedule="0 19 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    tags=["gold", "dbt"],
    default_args=DEFAULT_ARGS,
    doc_md="""
    ## gold_full_refresh

    Runs ``dbt run`` and ``dbt test`` for the whole project (no subset).
    Scheduled after ``structured_daily`` (16:30) and ``news_daily`` (06:00).
    """,
) as dag:
    dbt_run = PythonOperator(
        task_id="dbt_run_full",
        python_callable=dbt_run_full,
    )

    dbt_test = PythonOperator(
        task_id="dbt_test_full",
        python_callable=dbt_test_full,
    )

    dbt_run >> dbt_test
