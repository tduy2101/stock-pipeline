{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['symbol', 'trading_date'], 'type': 'btree', 'unique': true}
    ]
  )
}}

select
  symbol,
  trading_date,
  exchange,
  ceiling_price,
  floor_price,
  reference_price,
  close_price,
  open_price,
  high_price,
  low_price,
  average_price,
  price_change,
  percent_change,
  volume_accumulated,
  total_value,
  bid_price_1,
  bid_vol_1,
  bid_price_2,
  bid_vol_2,
  bid_price_3,
  bid_vol_3,
  ask_price_1,
  ask_vol_1,
  ask_price_2,
  ask_vol_2,
  ask_price_3,
  ask_vol_3,
  foreign_buy_volume,
  foreign_sell_volume,
  (foreign_buy_volume - foreign_sell_volume) as foreign_net_volume,
  foreign_room,
  round(
    case
      when bid_price_1 > 0
        then (ask_price_1 - bid_price_1) / bid_price_1 * 100
      else null
    end::numeric,
    4
  ) as spread_pct,
  snapshot_at
from {{ ref('stg_price_board') }}
where is_suspicious = false
  or is_suspicious is null
