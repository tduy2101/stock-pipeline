{{ config(materialized='table') }}

/*
RSI: Cutler's method (SMA-based rolling avg, not Wilder's EMA).
MACD: SMA12 - SMA26 approximation. Values NULL until window full.
*/

with base as (
  select
    ticker,
    trading_date,
    close,
    lag(close) over (
      partition by ticker
      order by trading_date
    ) as prev_close,
    count(*) over (
      partition by ticker
      order by trading_date
      rows between unbounded preceding and current row
    ) as row_num
  from {{ ref('stg_price') }}
),

returns as (
  select
    ticker,
    trading_date,
    close,
    prev_close,
    row_num,
    case
      when prev_close is not null and prev_close <> 0
        then (close - prev_close) / prev_close
      else null
    end as daily_return,
    greatest(close - prev_close, 0) as gain,
    greatest(prev_close - close, 0) as loss
  from base
),

windows as (
  select
    ticker,
    trading_date,
    close,
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
    ) as std20
  from returns
),

indicators as (
  select
    ticker,
    trading_date,
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
    case when row_num >= 20 then ma20_raw - 2 * std20 else null end as bb_lower
  from windows
),

final as (
  select
    ticker,
    trading_date,
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
    bb_lower
  from indicators
)

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
  current_timestamp as calculated_at
from final
