# Credit Card Transaction Data Pipeline & Quality Monitoring

An end-to-end analytics engineering pipeline that ingests raw credit card transaction
data, transforms it with PySpark, models it in Snowflake with dbt, validates it with
Great Expectations, and surfaces fraud/volume trends in a BI dashboard.

Built to mirror a real analytics-engineering workflow at a card issuer: raw data lands →
gets cleaned and transformed at scale → is modeled into trustworthy, documented tables →
is continuously checked for quality → is consumed by business stakeholders via dashboards.

---

## 1. Problem

Card issuers process millions of transactions a day. Before any fraud analysis, forecasting,
or executive reporting can be trusted, the underlying data has to be:

- Deduplicated and schema-consistent across ingestion batches
- Enriched with derived features (transaction hour, merchant category risk flags, rolling
  spend velocity per card)
- Continuously monitored for quality drift (nulls, out-of-range amounts, duplicate
  transaction IDs) — not just checked once at ingestion

This project simulates that pipeline end-to-end using a public-style synthetic dataset.

## 2. Approach

| Stage | Tool | What it does |
|---|---|---|
| Data generation | Python (`Faker`) | Generates realistic synthetic transaction data with intentional data-quality issues (nulls, dupes, outliers) so the quality layer has something to catch |
| Transformation | PySpark | Cleans, deduplicates, and engineers features at scale |
| Warehousing | Snowflake + dbt | Loads transformed data into a warehouse and models it into staging → mart layers with tests and documentation |
| Data quality | Great Expectations | Validates schema, null rates, value ranges, and duplicate rates — the open-source analog to Informatica DQ |
| Orchestration | Airflow | Schedules and sequences the pipeline daily |
| BI | Tableau / Power BI | Dashboards on transaction volume, fraud rate trends, and data quality pass/fail rates over time |

## 3. Architecture

```
Synthetic Data Generator (Faker)
        │
        ▼
   Raw CSV / Parquet (data/raw/)
        │
        ▼
  PySpark Transform Job  ──────► Cleaned Parquet (data/processed/)
        │
        ▼
  Load to Snowflake (RAW schema)
        │
        ▼
   dbt: staging models  ──────► dbt: mart models (fact_transactions, dim_cards)
        │
        ▼
  Great Expectations Validation Suite ──► Quality report (pass/fail, logged per run)
        │
        ▼
      Tableau / Power BI Dashboard
        │
      (Airflow DAG orchestrates all of the above daily)
```

See `architecture/pipeline_diagram.md` for a text-renderable version of this diagram.

## 4. Repo Structure

```
credit-card-pipeline/
├── data/
│   └── generate_synthetic_data.py     # Creates synthetic transactions w/ built-in DQ issues
├── src/
│   ├── ingest/
│   │   └── spark_transform.py         # PySpark cleaning + feature engineering
│   ├── quality/
│   │   └── ge_validation_suite.py     # Great Expectations checks
│   └── dashboard/
│       └── dashboard_notes.md         # Metrics + chart spec for Tableau/Power BI
├── dbt_project/
│   ├── models/staging/                # stg_transactions.sql
│   └── models/marts/                  # fact_transactions.sql, dim_cards.sql
├── airflow/
│   └── dags/pipeline_dag.py           # Daily orchestration DAG
├── architecture/
│   └── pipeline_diagram.md
├── requirements.txt
└── README.md
```

## 5. Results

- Processed 50,400 synthetic transactions through the full pipeline
- PySpark transform dropped 999 invalid records (nulls/malformed values) and 390 duplicate transactions, yielding 49,011 clean rows
- Great Expectations validation suite: 13/13 data quality checks passed (100% pass rate) on the cleaned dataset
- End-to-end pipeline (generation → Spark transform → quality validation) runs in under 2 minutes locally


## 6. How to Run

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Generate synthetic data
python data/generate_synthetic_data.py

# 3. Run the PySpark transform
python src/ingest/spark_transform.py

# 4. Run data quality validation
python src/quality/ge_validation_suite.py

# 5. (Optional) Load to Snowflake — requires your own Snowflake trial account
#    Set env vars: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, etc.
python src/ingest/load_to_snowflake.py

# 6. (Optional) Run dbt models against Snowflake
cd dbt_project && dbt run && dbt test

# 7. (Optional) Stand up Airflow locally and trigger the DAG
```

## 7. Why This Project

Built to demonstrate the parts of the analytics-engineering stack I hadn't yet shown in
production work: distributed transformation with **PySpark** and a formalized, named
**data quality framework** (Great Expectations), on top of skills I already use daily —
SQL, Snowflake, dbt, and Tableau.
