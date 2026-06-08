"""Monthly structured: bronze → silver (listing/company/financial_ratio) → load → dbt.

Runs full HOSE/HNX snapshots that don't need daily refresh: the listing master,
company overview, and financial ratios. Scheduled on the 1st of each month at
17:00 ICT (after the daily structured run finishes).
"""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from common.callbacks import on_failure_callback, on_success_callback
from common.tasks import (
    DBT_STRUCTURED_MONTHLY_SELECT,
    dbt_select,
    ingest_structured_monthly,
    load_silver,
    silver_company,
    silver_financial_ratio,
    silver_listing,
)

DEFAULT_ARGS = {
    "owner": "stock-pipeline",
    "retries": 0,
    "on_failure_callback": on_failure_callback,
    "on_success_callback": on_success_callback,
}

with DAG(
    dag_id="structured_monthly",
    description="Monthly full HOSE/HNX listing, company, financial_ratio bronze/silver/load/dbt",
    schedule="0 17 1 * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    tags=["structured", "monthly", "bronze", "silver", "gold"],
    default_args=DEFAULT_ARGS,
    doc_md="""
    ## structured_monthly

    **Bronze (monthly, ngày 1):** full HOSE/HNX universe — listing → company →
    financial_ratio. Bỏ qua price/index/price_board (đã chạy ở ``structured_daily``).

    **Silver:** listing → company → financial_ratio.

    **Load:** ``listing,company,financial_ratio`` → ``silver.*``

    **dbt:** ``+mart_financial_summary +mart_company_profile`` (kéo theo dim_listing,
    fact_price_daily upstream).

    **Rate limit:** vnstock ~50 rpm — serial tasks; bronze full universe ~hàng nghìn call.
    """,
) as dag:
    bronze_monthly = PythonOperator(
        task_id="bronze_structured_monthly",
        python_callable=ingest_structured_monthly,
        retries=2,
        retry_delay=pendulum.duration(minutes=15),
    )

    silver_listing_task = PythonOperator(
        task_id="silver_listing",
        python_callable=silver_listing,
    )
    silver_company_task = PythonOperator(
        task_id="silver_company",
        python_callable=silver_company,
    )
    silver_financial_ratio_task = PythonOperator(
        task_id="silver_financial_ratio",
        python_callable=silver_financial_ratio,
    )

    load_silver_task = PythonOperator(
        task_id="load_silver",
        python_callable=load_silver,
        op_kwargs={"datasets": "listing,company,financial_ratio"},
    )

    dbt_marts = PythonOperator(
        task_id="dbt_marts",
        python_callable=dbt_select,
        op_kwargs={"selectors": DBT_STRUCTURED_MONTHLY_SELECT},
    )

    (
        bronze_monthly
        >> silver_listing_task
        >> silver_company_task
        >> silver_financial_ratio_task
        >> load_silver_task
        >> dbt_marts
    )
