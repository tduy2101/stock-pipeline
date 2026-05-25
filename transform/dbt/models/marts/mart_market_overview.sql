{{ config(materialized='table') }}

with index_returns as (
  select
    index_code,
    trading_date,
    close,
    case
      when lag(close) over (
        partition by index_code
        order by trading_date
      ) <> 0
        then (
          close - lag(close) over (
            partition by index_code
            order by trading_date
          )
        ) / nullif(
          lag(close) over (
            partition by index_code
            order by trading_date
          ),
          0
        )
      else null
    end as index_return
  from {{ ref('fact_index_daily') }}
),

index_pivot as (
  select
    trading_date,
    max(case when index_code = 'VNINDEX' then close end) as vnindex_close,
    max(case when index_code = 'VNINDEX' then index_return end) as vnindex_return,
    max(case when index_code = 'VN30' then close end) as vn30_close,
    max(case when index_code = 'VN30' then index_return end) as vn30_return,
    max(case when index_code = 'HNXINDEX' then close end) as hnx_close,
    max(case when index_code = 'HNXINDEX' then index_return end) as hnx_return
  from index_returns
  group by trading_date
),

price_agg as (
  select
    trading_date,
    sum(volume) as total_volume,
    sum(value) as total_value,
    count(*) filter (where daily_return > 0) as advances,
    count(*) filter (where daily_return < 0) as declines,
    count(*) filter (where daily_return = 0) as unchanged
  from {{ ref('fact_price_daily') }}
  group by trading_date
),

top_movers as (
  select
    f1.trading_date,
    coalesce(
      (
        select json_agg(t)
        from (
          select
            f2.ticker,
            f2.close,
            round((f2.daily_return * 100)::numeric, 2) as percent_change
          from {{ ref('fact_price_daily') }} as f2
          where f2.trading_date = f1.trading_date
            and f2.daily_return is not null
          order by f2.daily_return desc
          limit 5
        ) as t
      ),
      '[]'::json
    ) as top_gainers,
    coalesce(
      (
        select json_agg(t)
        from (
          select
            f2.ticker,
            f2.close,
            round((f2.daily_return * 100)::numeric, 2) as percent_change
          from {{ ref('fact_price_daily') }} as f2
          where f2.trading_date = f1.trading_date
            and f2.daily_return is not null
          order by f2.daily_return asc
          limit 5
        ) as t
      ),
      '[]'::json
    ) as top_losers
  from (
    select distinct trading_date
    from {{ ref('fact_price_daily') }}
  ) as f1
)

select
  i.trading_date,
  i.vnindex_close,
  i.vnindex_return,
  i.vn30_close,
  i.vn30_return,
  i.hnx_close,
  i.hnx_return,
  p.total_value as total_market_value,
  p.total_volume,
  p.advances,
  p.declines,
  p.unchanged,
  m.top_gainers,
  m.top_losers
from index_pivot as i
inner join price_agg as p
  on i.trading_date = p.trading_date
left join top_movers as m
  on i.trading_date = m.trading_date
