{{ config(materialized='table') }}

select
  doc_id,
  ticker,
  year,
  period_key,
  title,
  normalized_title,
  published_at,
  doc_class,
  canonical_priority,
  is_consolidated,
  display_status,
  is_available_for_web,
  url_pdf,
  pdf_path,
  file_size
from {{ ref('stg_bctc_pdf_meta') }}
