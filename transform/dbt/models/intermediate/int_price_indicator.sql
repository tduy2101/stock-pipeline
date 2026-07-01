{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['ticker', 'trading_date'],
    on_schema_change='sync_all_columns',
    indexes=[
      {'columns': ['ticker', 'trading_date'], 'type': 'btree', 'unique': true}
    ]
  )
}}

/*
RSI: Cutler's method (SMA-based rolling avg, not Wilder's EMA).
MACD: SMA12 - SMA26 approximation. Values NULL until window full.
Incremental: 90-day lookback window per ticker with new data; OBV relative within window.
*/

with
{% if is_incremental() %}
watermark as (
  select coalesce(max(trading_date), '1900-01-01'::date) as max_date
  from {{ this }}
),

new_tickers as (
  select distinct p.ticker
  from {{ ref('stg_price') }} as p
  cross join watermark as w
  where p.trading_date > w.max_date
),

price_window as (
  select p.*
  from {{ ref('stg_price') }} as p
  inner join new_tickers as n using (ticker)
  cross join watermark as w
  where p.trading_date >= (w.max_date - interval '90 days')
),
{% endif %}

base as (
  select
    ticker,
    trading_date,
    close,
    volume,
    lag(close) over (
      partition by ticker
      order by trading_date
    ) as prev_close,
    count(*) over (
      partition by ticker
      order by trading_date
      rows between unbounded preceding and current row
    ) as row_num
  from {% if is_incremental() %}price_window{% else %}{{ ref('stg_price') }}{% endif %}
),

returns as (
  select
    ticker,
    trading_date,
    close,
    volume,
    prev_close,
    row_num,
    case
      when prev_close is not null and prev_close <> 0
        then (close - prev_close) / prev_close
      else null
    end as daily_return,
    greatest(close - prev_close, 0) as gain,
    greatest(prev_close - close, 0) as loss,
    case
      when close > prev_close then volume
      when close < prev_close then -volume
      else 0
    end as signed_volume
  from base
),

windows as (
  select
    ticker,
    trading_date,
    close,
    volume,
    signed_volume,
    prev_close,
    row_num,
    daily_return,
    avg(close) over (
      partition by ticker
      order by trading_date
      rows between 6 preceding and current row
    ) as ma7_raw,
    avg(close) over (
      partition by ticker
      order by trading_date
      rows between 19 preceding and current row
    ) as ma20_raw,
    avg(close) over (
      partition by ticker
      order by trading_date
      rows between 49 preceding and current row
    ) as ma50_raw,
    avg(gain) over (
      partition by ticker
      order by trading_date
      rows between 13 preceding and current row
    ) as avg_gain_14,
    avg(loss) over (
      partition by ticker
      order by trading_date
      rows between 13 preceding and current row
    ) as avg_loss_14,
    avg(close) over (
      partition by ticker
      order by trading_date
      rows between 11 preceding and current row
    ) as sma12,
    avg(close) over (
      partition by ticker
      order by trading_date
      rows between 25 preceding and current row
    ) as sma26,
    stddev_pop(close) over (
      partition by ticker
      order by trading_date
      rows between 19 preceding and current row
    ) as std20,
    avg(volume) over (
      partition by ticker
      order by trading_date
      rows between 19 preceding and current row
    ) as volume_ma20
  from returns
),

indicators as (
  select
    ticker,
    trading_date,
    signed_volume,
    daily_return,
    case when row_num >= 7 then ma7_raw else null end as ma7,
    case when row_num >= 20 then ma20_raw else null end as ma20,
    case when row_num >= 50 then ma50_raw else null end as ma50,
    case
      when row_num >= 14 and (avg_gain_14 + avg_loss_14) > 0
        then 100.0 - (100.0 / (1.0 + avg_gain_14 / nullif(avg_loss_14, 0)))
      else null
    end as rsi14,
    case when row_num >= 26 then sma12 - sma26 else null end as macd_line,
    case when row_num >= 20 then ma20_raw else null end as bb_middle,
    case when row_num >= 20 then ma20_raw + 2 * std20 else null end as bb_upper,
    case when row_num >= 20 then ma20_raw - 2 * std20 else null end as bb_lower,
    volume_ma20
  from windows
),

final as (
  select
    ticker,
    trading_date,
    signed_volume,
    daily_return,
    ma7,
    ma20,
    ma50,
    rsi14,
    macd_line,
    avg(macd_line) over (
      partition by ticker
      order by trading_date
      rows between 8 preceding and current row
    ) as macd_signal,
    bb_middle,
    bb_upper,
    bb_lower,
    volume_ma20
  from indicators
),

computed as (
  select
    ticker,
    trading_date,
    daily_return,
    ma7,
    ma20,
    ma50,
    rsi14,
    macd_line,
    macd_signal,
    macd_line - macd_signal as macd_hist,
    bb_middle,
    bb_upper,
    bb_lower,
    volume_ma20,
    sum(signed_volume) over (
      partition by ticker
      order by trading_date
      rows between unbounded preceding and current row
    ) as obv,
    current_timestamp as calculated_at
  from final
)

select *
from computed
{% if is_incremental() %}
where trading_date > (select max_date from watermark)
{% endif %}
