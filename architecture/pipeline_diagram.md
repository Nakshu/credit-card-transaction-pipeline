# Pipeline Architecture

```
┌───────────────────────────┐
│  Synthetic Data Generator  │   Faker-based transaction generator with
│  (data/generate_synthetic  │   intentional nulls / duplicates / outliers
│   _data.py)                │
└─────────────┬──────────────┘
              │  raw CSV
              ▼
┌───────────────────────────┐
│   PySpark Transform Job    │   Clean, deduplicate, engineer features
│  (src/ingest/spark_        │   (transaction_hour, card_daily_txn_count,
│   transform.py)            │    is_high_risk_category)
└─────────────┬──────────────┘
              │  cleaned Parquet
              ▼
┌───────────────────────────┐
│   Load to Snowflake        │   RAW schema table
│  (src/ingest/load_to_      │
│   snowflake.py)            │
└─────────────┬──────────────┘
              │
              ▼
┌───────────────────────────┐
│   dbt: staging models      │   stg_transactions
│  (dbt_project/models/      │
│   staging/)                │
└─────────────┬──────────────┘
              │
              ▼
┌───────────────────────────┐
│   dbt: mart models         │   fact_transactions, dim_cards
│  (dbt_project/models/      │
│   marts/)                  │
└─────────────┬──────────────┘
              │
              ▼
┌───────────────────────────┐
│  Great Expectations        │   Schema, null, range, uniqueness,
│  Validation Suite           │   categorical-domain checks
│  (src/quality/ge_           │
│   validation_suite.py)      │
└─────────────┬──────────────┘
              │  pass/fail results (JSON)
              ▼
┌───────────────────────────┐
│  Tableau / Power BI          │   Volume, fraud, and data-quality dashboards
│  Dashboard                   │
└───────────────────────────┘

     All stages orchestrated daily by the Airflow DAG in
     airflow/dags/pipeline_dag.py
```
