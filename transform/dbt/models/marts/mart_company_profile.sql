{{ config(materialized='table') }}

with latest_price as (
  select distinct on (ticker)
    ticker,
    close as latest_close,
    trading_date as latest_trading_date
  from {{ ref('fact_price_daily') }}
  order by ticker, trading_date desc
),

max_date as (
  select max(trading_date) as max_trading_date
  from {{ ref('fact_price_daily') }}
),

stats_52w as (
  select
    p.ticker,
    max(p.close) as high_52w,
    min(p.close) as low_52w
  from {{ ref('fact_price_daily') }} as p
  cross join max_date as d
  where p.trading_date >= d.max_trading_date - interval '365 days'
  group by p.ticker
),

ranked_volume as (
  select
    ticker,
    volume,
    row_number() over (
      partition by ticker
      order by trading_date desc
    ) as rn
  from {{ ref('fact_price_daily') }}
),

avg_vol_20d as (
  select
    ticker,
    avg(volume) as avg_volume_20d
  from ranked_volume
  where rn <= 20
  group by ticker
),

latest_ratio_period as (
  select distinct on (ticker)
    ticker,
    period
  from {{ ref('stg_financial_ratio') }}
  where item_code in ('pe_ratio', 'pb_ratio', 'trailing_eps', 'roe', 'roa')
    and value is not null
  order by ticker, period desc
),

latest_ratios as (
  select
    r.ticker,
    max(case when r.item_code = 'pe_ratio' then r.value end) as pe_ratio,
    max(case when r.item_code = 'pb_ratio' then r.value end) as pb_ratio,
    max(case when r.item_code = 'trailing_eps' then r.value end) as eps,
    max(case when r.item_code = 'roe' then r.value end) as roe,
    max(case when r.item_code = 'roa' then r.value end) as roa
  from {{ ref('stg_financial_ratio') }} as r
  inner join latest_ratio_period as p
    on r.ticker = p.ticker
    and r.period = p.period
  group by r.ticker
)

select
  c.ticker,
  c.symbol,
  c.exchange,
  c.company_name,
  c.short_name,
  c.industry,
  c.sector,
  c.charter_capital,
  c.established_year,
  c.listed_date,
  c.website,
  c.description,
  c.organ_name,
  c.en_organ_name,
  lp.latest_close,
  lp.latest_trading_date,
  s.high_52w,
  s.low_52w,
  v.avg_volume_20d,
  lr.pe_ratio,
  lr.pb_ratio,
  lr.eps,
  lr.roe,
  lr.roa
from {{ ref('dim_company') }} as c
left join latest_price as lp using (ticker)
left join stats_52w as s using (ticker)
left join avg_vol_20d as v using (ticker)
left join latest_ratios as lr using (ticker)
