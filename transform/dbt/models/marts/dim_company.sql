{{ config(materialized='table') }}

select
  c.ticker,
  c.symbol,
  c.exchange,
  coalesce(c.company_name, l.organ_name) as company_name,
  c.short_name,
  c.industry,
  c.sector,
  c.charter_capital,
  c.established_year,
  c.listed_date,
  c.website,
  c.description,
  l.organ_name,
  l.en_organ_name
from {{ ref('stg_company') }} as c
left join {{ ref('stg_listing') }} as l
  on c.symbol = l.symbol
