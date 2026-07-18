# Dashboard Spec

Connect Tableau or Power BI directly to the dbt mart models
(`fact_transactions`, `dim_cards`) in Snowflake. Suggested pages:

## Page 1 — Transaction Volume & Spend
- Line chart: daily transaction count and total spend over time
- Bar chart: spend by `merchant_category`
- Filter: `transaction_value_band` (low/medium/high)

## Page 2 — Fraud Monitoring
- KPI tiles: overall fraud rate, fraud-flagged transaction count (trailing 30 days)
- Line chart: fraud rate trend by day, split by `is_high_risk_category`
- Table: top 10 cards by `fraud_rate` (from `dim_cards`)

## Page 3 — Data Quality
- Pull from the JSON files in `src/quality/results/` (or load them into a
  small Snowflake table via a follow-up script) to chart:
  - Data quality pass rate over time (one point per pipeline run)
  - Count of failed expectations by type, most recent run
  - Row counts dropped at each pipeline stage (nulls removed, duplicates removed)
    — pulled from the `spark_transform.py` console summary if you choose to
    log it to a table instead of just stdout

This page is the one to walk an interviewer through — it's the direct
visual proof of the data-quality framework, not just the ETL.

## Suggested screenshot for your GitHub README
Once built, export a static image of Page 2 or Page 3 and embed it in the
top-level README so the project is skimmable without opening Tableau.
