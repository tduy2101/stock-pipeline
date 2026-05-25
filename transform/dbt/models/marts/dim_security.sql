{{ config(materialized='table') }}

select
  symbol,
  organ_name,
  en_organ_name,
  exchange,
  security_type
from {{ ref('stg_listing') }}
