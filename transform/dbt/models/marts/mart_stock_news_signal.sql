{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['ticker', 'trading_date'], 'type': 'btree', 'unique': true}
    ]
  )
}}

with base as (
  select
    ticker,
    article_id,
    title,
    url,
    published_at,
    published_date,
    sentiment_label,
    sentiment_score,
    source_tier,
    ticker_relevance,
    case
      when extract(dow from (published_at at time zone 'Asia/Ho_Chi_Minh')) = 0
        then published_date + interval '1 day'
      when extract(dow from (published_at at time zone 'Asia/Ho_Chi_Minh')) = 6
        then published_date + interval '2 days'
      when (published_at at time zone 'Asia/Ho_Chi_Minh')::time > '14:30:00'
        then published_date + interval '1 day'
      else published_date
    end::date as trading_date,
    (
      case ticker_relevance
        when 'title' then 3.0
        when 'summary' then 2.0
        else 1.0
      end
      * case source_tier
          when 1 then 1.5
          when 2 then 1.2
          else 1.0
        end
      * coalesce(
          exp(
            -extract(epoch from (current_timestamp - published_at))
            / (48.0 * 3600.0)
          ),
          1.0
        )
    ) as article_weight
  from {{ ref('fact_news_article') }}
  where url is not null
    and title is not null
),

aggregated as (
  select
    ticker,
    trading_date,
    count(*) as news_count,
    count(*) filter (where sentiment_label = 'positive') as positive_count,
    count(*) filter (where sentiment_label = 'negative') as negative_count,
    count(*) filter (where sentiment_label = 'neutral') as neutral_count,
    round(
      (
        sum(sentiment_score * article_weight)
        / nullif(sum(article_weight), 0)
      )::numeric,
      4
    ) as weighted_sentiment,
    round(avg(sentiment_score)::numeric, 4) as avg_sentiment_score,
    case
      when sum(sentiment_score * article_weight)
           / nullif(sum(article_weight), 0) >= 0.3
        then 'buy_signal'
      when sum(sentiment_score * article_weight)
           / nullif(sum(article_weight), 0) <= -0.3
        then 'sell_signal'
      else 'neutral'
    end as news_signal,
    case
      when count(*) filter (where sentiment_label = 'positive') * 1.0
           > count(*) filter (where sentiment_label = 'negative')
        then 'positive'
      when count(*) filter (where sentiment_label = 'negative') * 1.0
           > count(*) filter (where sentiment_label = 'positive')
        then 'negative'
      else 'neutral'
    end as dominant_sentiment,
    (
      select jsonb_agg(article order by (article->>'weight')::numeric desc)
      from (
        select jsonb_build_object(
          'article_id', b2.article_id,
          'title', b2.title,
          'url', b2.url,
          'sentiment', b2.sentiment_label,
          'published_at', b2.published_at,
          'relevance', b2.ticker_relevance,
          'weight', round(b2.article_weight::numeric, 4)
        ) as article
        from base as b2
        where b2.ticker = base.ticker
          and b2.trading_date = base.trading_date
        order by b2.article_weight desc nulls last
        limit 3
      ) as ranked_articles
    ) as top_articles
  from base
  group by ticker, trading_date
)

select *
from aggregated
