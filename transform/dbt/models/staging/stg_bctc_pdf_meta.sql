{{ config(materialized='ephemeral') }}

select
  doc_id,
  ticker,
  year,
  period_key,
  title,
  published_at,
  url_pdf,
  pdf_path,
  file_size,
  doc_class,
  is_consolidated,
  display_status,
  is_available_for_web
from {{ source('silver', 'bctc_pdf_meta') }}
where display_status != 'error'
  and is_available_for_web = true
  and doc_id is not null
