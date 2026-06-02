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
  c.free_float_percentage,
  c.free_float,
  c.number_of_employees,
  c.founded_date,
  c.ceo_name,
  c.ceo_position,
  c.outstanding_shares,
  c.auditor,
  c.established_year,
  c.listed_date,
  c.website,
  c.description,
  l.organ_name,
  l.en_organ_name
from {{ ref('stg_company') }} as c
left join {{ ref('stg_listing') }} as l
  on c.symbol = l.symbol
