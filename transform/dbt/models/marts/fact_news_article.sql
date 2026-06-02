{{ config(materialized='table') }}

select
  article_id,
  ticker,
  ticker_mentions,
  title,
  summary,
  body_text,
  url,
  source,
  published_at,
  published_date,
  sentiment_score,
  sentiment_label,
  word_count,
  language
from {{ ref('stg_news') }}
