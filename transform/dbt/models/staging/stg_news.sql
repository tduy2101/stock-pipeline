{{ config(materialized='ephemeral') }}

with base as (
  select
    article_id,
    source,
    ticker,
    ticker_mentions,
    title,
    summary,
    body_text,
    url,
    published_at,
    published_date,
    sentiment_score,
    sentiment_label,
    word_count,
    language
  from {{ source('silver', 'news') }}
  where published_date is not null
    and article_id is not null
    and title is not null
    and trim(title) <> ''
)

select
  article_id,
  source,
  coalesce(
    nullif(trim(ticker), ''),
    case
      when ticker_mentions is not null and cardinality(ticker_mentions) > 0
      then ticker_mentions[1]
    end
  ) as ticker,
  ticker_mentions,
  title,
  summary,
  body_text,
  url,
  published_at,
  published_date,
  sentiment_score,
  sentiment_label,
  word_count,
  language
from base
