{{ config(materialized='table') }}

select
  ticker,
  published_date,
  count(*) as news_count,
  avg(sentiment_score) as avg_sentiment_score,
  count(*) filter (where sentiment_label = 'positive') as positive_count,
  count(*) filter (where sentiment_label = 'negative') as negative_count,
  count(*) filter (where sentiment_label = 'neutral') as neutral_count,
  mode() within group (order by sentiment_label) as dominant_sentiment
from {{ ref('stg_news') }}
group by ticker, published_date
