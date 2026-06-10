{{
  config(
    materialized='table',
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
