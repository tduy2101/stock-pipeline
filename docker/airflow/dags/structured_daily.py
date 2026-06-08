"""Daily structured: bronze → silver (price/index/board) → load → dbt (T2–T6 16:30 ICT)."""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from common.callbacks import on_failure_callback, on_success_callback
from common.tasks import (
    DBT_STRUCTURED_SELECT,
    dbt_select,
    ingest_structured,
    load_silver,
    silver_index_price,
    silver_price,
    silver_price_board,
)

DEFAULT_ARGS = {
    "owner": "stock-pipeline",
    "retries": 0,
    "on_failure_callback": on_failure_callback,
    "on_success_callback": on_success_callback,
}

with DAG(
    dag_id="structured_daily",
    description="Structured bronze/silver/load/dbt — listing, price, index, company, price_board",
    schedule="30 16 * * 1-5",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    tags=["structured", "bronze", "silver", "gold"],
    default_args=DEFAULT_ARGS,
    doc_md="""
    ## structured_daily

    **Bronze (daily):** chỉ **price + index + price_board**, mặc định **50 mã cố định**
    (``STRUCTURED_DAG_UNIVERSE=watchlist50`` = ``DEFAULT_PRICE_BOARD_TICKERS``). listing &
    company được xử lý ở ``structured_monthly``. Đổi ``STRUCTURED_DAG_UNIVERSE=listing``
    (+ tuỳ chọn ``STRUCTURED_MAX_TICKERS``) cho full HOSE/HNX price.

    **Silver:** price → index_price → price_board.

    **Load:** ``price,index_price,price_board`` → ``silver.*``

    **dbt:** ``+mart_stock_daily +mart_price_board +mart_market_overview``
    (``mart_stock_daily`` joins ``mart_stock_news_signal`` from prior ``news_daily`` runs;
    listing dim & company profile refreshed by ``structured_monthly``).

    **Rate limit:** vnstock ~50 rpm — serial tasks only; bronze retries on rate limit.
    """,
) as dag:
    bronze_structured = PythonOperator(
        task_id="bronze_structured",
        python_callable=ingest_structured,
        retries=2,
        retry_delay=pendulum.duration(minutes=15),
    )

    silver_price_task = PythonOperator(
        task_id="silver_price",
        python_callable=silver_price,
    )
    silver_index_price_task = PythonOperator(
        task_id="silver_index_price",
        python_callable=silver_index_price,
    )
    silver_price_board_task = PythonOperator(
        task_id="silver_price_board",
        python_callable=silver_price_board,
    )

    load_silver_task = PythonOperator(
        task_id="load_silver",
        python_callable=load_silver,
        op_kwargs={
            "datasets": "price,index_price,price_board",
        },
    )

    dbt_marts = PythonOperator(
        task_id="dbt_marts",
        python_callable=dbt_select,
        op_kwargs={"selectors": DBT_STRUCTURED_SELECT},
    )

    (
        bronze_structured
        >> silver_price_task
        >> silver_index_price_task
        >> silver_price_board_task
        >> load_silver_task
        >> dbt_marts
    )
