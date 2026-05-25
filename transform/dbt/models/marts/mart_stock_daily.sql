{{ config(materialized='table') }}

select
  fpd.*,
  coalesce(ns.news_count, 0) as news_count,
  ns.avg_sentiment_score,
  ns.dominant_sentiment
from {{ ref('fact_price_daily') }} fpd
left join {{ ref('int_news_sentiment_daily') }} ns
  on fpd.ticker = ns.ticker
  and fpd.trading_date = ns.published_date
