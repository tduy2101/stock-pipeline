{{ config(materialized='table') }}

with ticker_universe as (
  select distinct upper(symbol) as ticker
  from {{ ref('dim_security') }}
  where symbol is not null

  union

  select distinct upper(ticker) as ticker
  from {{ ref('mart_company_profile') }}
  where ticker is not null

  union

  select distinct upper(ticker) as ticker
  from {{ ref('fact_price_daily') }}
  where ticker is not null

  union

  select distinct upper(ticker) as ticker
  from {{ ref('fact_news_article') }}
  where ticker is not null

  union

  select distinct upper(ticker) as ticker
  from {{ ref('mart_bctc_documents') }}
  where ticker is not null
),

price_stats as (
  select
    upper(ticker) as ticker,
    count(*)::int as price_count,
    max(trading_date) as latest_trading_date
  from {{ ref('fact_price_daily') }}
  group by upper(ticker)
),

news_stats as (
  select
    upper(ticker) as ticker,
    count(*)::int as news_count,
    max(published_at) as latest_news_at
  from {{ ref('fact_news_article') }}
  group by upper(ticker)
),

bctc_stats as (
  select
    upper(ticker) as ticker,
    count(*)::int as bctc_doc_count,
    max(published_at) as latest_bctc_at
  from {{ ref('mart_bctc_documents') }}
  group by upper(ticker)
)

select
  u.ticker,
  coalesce(cp.exchange, ds.exchange) as exchange,
  coalesce(cp.organ_name, cp.company_name, ds.organ_name, ds.en_organ_name) as organ_name,
  ds.en_organ_name,
  ds.security_type,
  cp.company_name,
  cp.short_name,
  (cp.ticker is not null) as has_full_profile,
  (ps.price_count is not null and ps.price_count > 0) as has_price,
  (ns.news_count is not null and ns.news_count > 0) as has_news,
  (bs.bctc_doc_count is not null and bs.bctc_doc_count > 0) as has_bctc,
  coalesce(ns.news_count, 0)::int as news_count,
  coalesce(bs.bctc_doc_count, 0)::int as bctc_doc_count,
  ps.latest_trading_date,
  ns.latest_news_at,
  bs.latest_bctc_at
from ticker_universe as u
left join {{ ref('dim_security') }} as ds
  on u.ticker = upper(ds.symbol)
left join {{ ref('mart_company_profile') }} as cp
  on u.ticker = upper(cp.ticker)
left join price_stats as ps
  on u.ticker = ps.ticker
left join news_stats as ns
  on u.ticker = ns.ticker
left join bctc_stats as bs
  on u.ticker = bs.ticker
