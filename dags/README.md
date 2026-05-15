# Airflow DAGs

Planned DAGs:

- `dag_structure_daily.py`
- `dag_news_daily.py`
- `dag_bctc_quarterly.py`

These DAGs should import existing ingestion modules as-is, then call Silver transforms/loaders and dbt Gold jobs.
