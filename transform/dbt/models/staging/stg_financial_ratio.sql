{{ config(materialized='view') }}

select
  ticker,
  period,
  period_type,
  year,
  quarter,
  item_code,
  item_name,
  value::double precision as value,
  source,
  snapshot_date,
  fetched_at,
  run_partition,
  source_file
from {{ source('silver', 'financial_ratio') }}
