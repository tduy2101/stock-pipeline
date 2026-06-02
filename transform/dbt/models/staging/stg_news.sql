{{
  config(
    materialized='ephemeral'
  )
}}

with base as (
  select *
  from {{ source('silver', 'news') }}
  where published_date is not null
    and article_id is not null
    and title is not null
    and trim(title) <> ''
),

unified_tickers as (
  select
    article_id,
    source,
    title,
    summary,
    body_text,
    url,
    published_at,
    published_date,
    sentiment_score,
    sentiment_label,
    word_count,
    language,
    ticker_mentions,
    array_remove(
      array(
        select distinct trim(t)
        from unnest(
          array_cat(
            case
              when ticker is not null and trim(ticker) <> ''
                then array[trim(ticker)]
              else array[]::text[]
            end,
            coalesce(ticker_mentions, array[]::text[])
          )
        ) as t
        where t is not null
          and trim(t) <> ''
      ),
      null
    ) as all_tickers
  from base
),

exploded as (
  select
    u.article_id,
    u.source,
    t.ticker,
    u.ticker_mentions,
    u.title,
    u.summary,
    u.body_text,
    u.url,
    u.published_at,
    u.published_date,
    u.sentiment_score,
    u.sentiment_label,
    u.word_count,
    u.language,
    case
      when lower(u.title) like '%' || lower(t.ticker) || '%' then 'title'
      when lower(u.summary) like '%' || lower(t.ticker) || '%' then 'summary'
      else 'body'
    end as ticker_relevance,
    case
      when u.source ilike '%vietstock%' then 1
      when u.source ilike '%cafef%' then 2
      else 3
    end as source_tier
  from unified_tickers as u,
       unnest(u.all_tickers) as t(ticker)
  where array_length(u.all_tickers, 1) > 0
)

select distinct on (article_id, ticker)
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
  language,
  ticker_relevance,
  source_tier
from exploded
order by
  article_id,
  ticker,
  case ticker_relevance
    when 'title' then 1
    when 'summary' then 2
    else 3
  end
