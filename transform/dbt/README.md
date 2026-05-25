# dbt Project

This dbt project reads PostgreSQL schema `silver` and builds analytics models in
schema `gold`.

Run from repo root:

```powershell
dbt debug --profiles-dir transform/dbt
dbt compile --profiles-dir transform/dbt
dbt run --profiles-dir transform/dbt
dbt test --profiles-dir transform/dbt
```

Run from this directory:

```powershell
dbt run --profiles-dir .
dbt test --profiles-dir .
```

Model layers:

```text
silver sources
  -> staging
  -> intermediate
  -> marts/facts/dimensions
```

Current key marts:

- `mart_stock_daily`
- `mart_company_profile`
- `mart_market_overview`
- `mart_stock_news_daily`
- `mart_bctc_documents`

Detailed dbt flow notes are in `../../Summary Flow/GOLD_DBT_FLOW.md`.
