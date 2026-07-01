{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['ticker', 'trading_date'],
    on_schema_change='sync_all_columns',
    indexes=[
      {'columns': ['ticker', 'trading_date'], 'type': 'btree', 'unique': true},
      {'columns': ['trading_date'], 'type': 'btree'}
    ]
  )
}}

select
  fpd.*,
  coalesce(ns.news_count, 0) as news_count,
  ns.avg_sentiment_score,
  ns.weighted_sentiment,
  ns.dominant_sentiment,
  ns.news_signal,
  ns.top_articles
from {{ ref('fact_price_daily') }} as fpd
left join {{ ref('mart_stock_news_signal') }} as ns
  on fpd.ticker = ns.ticker
  and fpd.trading_date = ns.trading_date
{% if is_incremental() %}
where fpd.trading_date > (
  select coalesce(max(trading_date), '1900-01-01'::date)
  from {{ this }}
)
{% endif %}
