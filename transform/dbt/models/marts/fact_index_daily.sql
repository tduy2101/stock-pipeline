{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['index_code', 'trading_date'], 'type': 'btree', 'unique': true},
      {'columns': ['trading_date'], 'type': 'btree'}
    ]
  )
}}

select
  index_code,
  trading_date,
  open,
  high,
  low,
  close,
  volume
from {{ ref('stg_index_price') }}
