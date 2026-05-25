{{ config(materialized='view') }}

select
  symbol,
  organ_name,
  en_organ_name,
  exchange,
  security_type,
  source,
  crawled_at,
  run_partition,
  source_file
from {{ source('silver', 'listing') }}
