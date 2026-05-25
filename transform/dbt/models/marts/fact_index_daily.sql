{{ config(materialized='table') }}

select
  index_code,
  trading_date,
  open,
  high,
  low,
  close,
  volume
from {{ ref('stg_index_price') }}
