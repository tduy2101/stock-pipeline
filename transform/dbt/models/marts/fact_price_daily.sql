{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['ticker', 'trading_date'],
    on_schema_change='sync_all_columns',
    indexes=[
      {'columns': ['ticker', 'trading_date'], 'type': 'btree', 'unique': true},
      {'columns': ['trading_date'], 'type': 'btree'},
      {'columns': ['trading_date', 'daily_return'], 'type': 'btree'}
    ]
  )
}}

select
  p.ticker,
  p.trading_date,
  p.open,
  p.high,
  p.low,
  p.close,
  p.volume,
  p.value,
  p.value_is_derived,
  p.source,
  p.instrument_type,
  p.fetched_at,
  p.is_suspicious,
  p.bronze_ingested_at,
  p.run_partition,
  p.source_file,
  i.daily_return,
  i.ma7,
  i.ma20,
  i.ma50,
  i.rsi14,
  i.macd_line,
  i.macd_signal,
  i.macd_hist,
  i.bb_middle,
  i.bb_upper,
  i.bb_lower,
  i.volume_ma20,
  i.obv,
  i.calculated_at
from {{ ref('stg_price') }} as p
left join {{ ref('int_price_indicator') }} as i
  on p.ticker = i.ticker
 and p.trading_date = i.trading_date
{% if is_incremental() %}
where p.trading_date > (
  select coalesce(max(trading_date), '1900-01-01'::date)
  from {{ this }}
)
{% endif %}
