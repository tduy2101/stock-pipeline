"""Daily news: bronze → silver → load → dbt (06:00 ICT)."""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from common.callbacks import on_failure_callback, on_success_callback
from common.tasks import (
    DBT_NEWS_SELECT,
    dbt_select,
    ingest_news,
    load_silver,
    silver_news_from_xcom,
)

DEFAULT_ARGS = {
    "owner": "stock-pipeline",
    "retries": 0,
    "on_failure_callback": on_failure_callback,
    "on_success_callback": on_success_callback,
}

with DAG(
    dag_id="news_daily",
    description="News RSS/HTML bronze, silver partition, load, dbt news marts",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    tags=["news", "bronze", "silver", "gold"],
    default_args=DEFAULT_ARGS,
    doc_md="""
    ## news_daily

    **Bronze:** ``NewsIngestionConfig()`` — ``days_back=1``, rate limit 20 rpm.

    **Silver:** ``--dataset news --run-partition <XCom> --strict`` (partition = ingest date).

    **Load:** ``news`` → ``silver.news``

    **dbt:** ``+mart_stock_news_signal +fact_news_article`` (feeds ``mart_stock_daily`` join).
    """,
) as dag:
    bronze_news = PythonOperator(
        task_id="bronze_news",
        python_callable=ingest_news,
        retries=1,
        retry_delay=pendulum.duration(minutes=5),
    )

    silver_news = PythonOperator(
        task_id="silver_news",
        python_callable=silver_news_from_xcom,
    )

    load_silver_task = PythonOperator(
        task_id="load_silver",
        python_callable=load_silver,
        op_kwargs={"datasets": "news"},
    )

    dbt_marts = PythonOperator(
        task_id="dbt_marts",
        python_callable=dbt_select,
        op_kwargs={"selectors": DBT_NEWS_SELECT},
    )

    bronze_news >> silver_news >> load_silver_task >> dbt_marts
