{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['ticker', 'period'], 'type': 'btree'}
    ]
  )
}}

with period_rows as (
  select
    ticker,
    period,
    period_type,
    year,
    quarter,
    max(case when item_code = 'pe_ratio' then value end) as pe_ratio,
    max(case when item_code = 'pb_ratio' then value end) as pb_ratio,
    max(case when item_code = 'ps_ratio' then value end) as ps_ratio,
    max(case when item_code = 'ev_ebit' then value end) as ev_ebit,
    max(case when item_code = 'ev_ebitda' then value end) as ev_ebitda,
    max(case when item_code = 'trailing_eps' then value end) as eps,
    max(case when item_code = 'roe' then value end) as roe,
    max(case when item_code = 'roe_trailling' then value end) as roe_trailing,
    max(case when item_code = 'roa' then value end) as roa,
    max(case when item_code = 'roa_trailling' then value end) as roa_trailing,
    max(case when item_code = 'return_on_capital_employed_roce' then value end) as roce,
    max(case when item_code = 'gross_margin' then value end) as gross_profit_margin,
    max(case when item_code = 'net_margin' then value end) as net_profit_margin,
    max(case when item_code = 'ebit_margin' then value end) as ebit_margin,
    max(case when item_code = 'ebitda_net_revenue' then value end) as ebitda_margin,
    max(case when item_code = 'short_term_ratio' then value end) as current_ratio,
    max(case when item_code = 'quick_ratio' then value end) as quick_ratio,
    max(case when item_code = 'cash_ratio' then value end) as cash_ratio,
    max(case when item_code = 'debt_to_equity' then value end) as debt_to_equity,
    max(case when item_code = 'debt_to_assets' then value end) as debt_to_assets,
    max(case when item_code = 'liabilities_to_equity' then value end) as liabilities_to_equity,
    max(case when item_code = 'liabilities_to_assets' then value end) as liabilities_to_assets,
    max(case when item_code = 'net_revenue' then value end) as revenue_growth,
    max(case when item_code = 'gross_profit' then value end) as gross_profit_growth,
    max(
      case
        when item_code = 'profit_after_tax_for_shareholders_of_the_parent_company'
          then value
      end
    ) as profit_growth,
    max(case when item_code = 'dividend_yield' then value end) as dividend_yield,
    null::double precision as dividend_per_share,
    max(case when item_code = 'book_value_per_share_bvps' then value end) as book_value_per_share,
    max(case when item_code = 'cash_flow_per_share_cps' then value end) as cash_flow_per_share,
    max(case when item_code = 'beta' then value end) as beta
  from {{ ref('stg_financial_ratio') }}
  group by ticker, period, period_type, year, quarter
),

latest_quarter_per_year as (
  select *
  from (
    select
      period_rows.*,
      row_number() over (
        partition by ticker, year
        order by quarter desc nulls last, period desc
      ) as rn
    from period_rows
    where period_type = 'quarter'
      and year is not null
  ) as ranked
  where rn = 1
),

annual_rows as (
  select
    ticker,
    year::text as period,
    'annual' as period_type,
    year,
    null::integer as quarter,
    pe_ratio,
    pb_ratio,
    ps_ratio,
    ev_ebit,
    ev_ebitda,
    eps,
    roe,
    roe_trailing,
    roa,
    roa_trailing,
    roce,
    gross_profit_margin,
    net_profit_margin,
    ebit_margin,
    ebitda_margin,
    current_ratio,
    quick_ratio,
    cash_ratio,
    debt_to_equity,
    debt_to_assets,
    liabilities_to_equity,
    liabilities_to_assets,
    revenue_growth,
    gross_profit_growth,
    profit_growth,
    dividend_yield,
    dividend_per_share,
    book_value_per_share,
    cash_flow_per_share,
    beta
  from latest_quarter_per_year
)

select *
from period_rows

union all

select *
from annual_rows
