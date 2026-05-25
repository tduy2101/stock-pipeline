{{ config(materialized='ephemeral') }}

select
  article_id,
  source,
  ticker,
  ticker_mentions,
  title,
  summary,
  url,
  published_at,
  published_date,
  sentiment_score,
  sentiment_label,
  word_count,
  language
from {{ source('silver', 'news') }}
where published_date is not null
  and ticker is not null
  and article_id is not null
