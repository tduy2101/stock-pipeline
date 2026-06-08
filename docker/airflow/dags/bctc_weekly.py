"""Weekly BCTC PDF: bronze → silver → load → dbt (Sat 10:00 ICT)."""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from common.callbacks import on_failure_callback, on_success_callback
from common.tasks import (
    DBT_BCTC_SELECT,
    dbt_select,
    ingest_bctc,
    load_silver,
    silver_bctc_from_xcom,
)

DEFAULT_ARGS = {
    "owner": "stock-pipeline",
    "retries": 0,
    "on_failure_callback": on_failure_callback,
    "on_success_callback": on_success_callback,
}

with DAG(
    dag_id="bctc_weekly",
    description="BCTC HNX crawl/download, silver bctc_pdf_meta, load, dbt mart",
    schedule="0 10 * * 6",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    tags=["bctc", "bronze", "silver", "gold"],
    default_args=DEFAULT_ARGS,
    doc_md="""
    ## bctc_weekly

    **Bronze:** ``SemiStructuredIngestionConfig()`` — ``hnx_max_list_pages=10``, rate 10 rpm.

    **Silver:** ``--dataset bctc_pdf_meta --run-partition <XCom>``.

    **Load:** ``bctc_pdf_meta`` → ``silver.bctc_pdf_meta``

    **dbt:** ``+mart_bctc_documents``
    """,
) as dag:
    bronze_bctc = PythonOperator(
        task_id="bronze_bctc",
        python_callable=ingest_bctc,
        retries=1,
        retry_delay=pendulum.duration(minutes=10),
    )

    silver_bctc = PythonOperator(
        task_id="silver_bctc",
        python_callable=silver_bctc_from_xcom,
    )

    load_silver_task = PythonOperator(
        task_id="load_silver",
        python_callable=load_silver,
        op_kwargs={"datasets": "bctc_pdf_meta"},
    )

    dbt_marts = PythonOperator(
        task_id="dbt_marts",
        python_callable=dbt_select,
        op_kwargs={"selectors": DBT_BCTC_SELECT},
    )

    bronze_bctc >> silver_bctc >> load_silver_task >> dbt_marts
