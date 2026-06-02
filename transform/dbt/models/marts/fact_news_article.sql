{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['ticker'], 'type': 'btree'},
      {'columns': ['published_date'], 'type': 'btree'},
      {'columns': ['ticker', 'published_date'], 'type': 'btree'}
    ]
  )
}}

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
  language,
  ticker_relevance,
  source_tier
from {{ ref('stg_news') }}
