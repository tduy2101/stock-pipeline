{{ config(materialized='table') }}

select
  ticker,
  published_date,
  news_count,
  round(avg_sentiment_score::numeric, 4) as avg_sentiment_score,
  positive_count,
  negative_count,
  neutral_count,
  dominant_sentiment
from {{ ref('int_news_sentiment_daily') }}
