{{ config(materialized='view') }}

select
  ticker,
  trading_date,
  open::double precision as open,
  high::double precision as high,
  low::double precision as low,
  close::double precision as close,
  volume::bigint as volume,
  value::double precision as value,
  value_is_derived,
  source,
  instrument_type,
  fetched_at,
  is_suspicious,
  bronze_ingested_at,
  run_partition,
  source_file
from {{ source('silver', 'price') }}
