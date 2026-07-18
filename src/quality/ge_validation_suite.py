"""
Data quality validation suite using Great Expectations.

This is the open-source equivalent of the Informatica DQ-style checks
mentioned in Capital One's Data Analyst postings ("knowledge of data
governance, data quality management concepts and data quality tools").

It validates the cleaned Parquet output from spark_transform.py against a
set of expectations covering:
  - schema / column presence
  - null rates on critical fields
  - value ranges (transaction_amount must be positive)
  - uniqueness (transaction_id must have no duplicates post-clean)
  - categorical domain checks (merchant_category must be a known value)

Run: python src/quality/ge_validation_suite.py

Produces a pass/fail summary printed to console and a JSON results file in
src/quality/results/ -- this JSON is what the dashboard's "data quality
pass rate over time" chart is built from (see src/dashboard/dashboard_notes.md).
"""

import glob
import json
import os
from datetime import datetime

import pandas as pd
import great_expectations as gx

PROCESSED_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "data", "processed")
RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")

VALID_CATEGORIES = [
    "grocery", "electronics", "travel", "restaurant", "gas_station",
    "online_retail", "utilities", "entertainment", "pharmacy", "jewelry",
]


def load_processed_data() -> pd.DataFrame:
    files = glob.glob(os.path.join(PROCESSED_PATH, "**", "*.parquet"), recursive=True)
    if not files:
        raise FileNotFoundError(
            "No processed Parquet files found. Run src/ingest/spark_transform.py first."
        )
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def build_and_run_validation(context, df):
    """
    Great Expectations 1.x uses a different API than 0.18.x:
    Data Source -> Data Asset -> Batch Definition -> Expectation Suite ->
    Validation Definition -> run(). This replaces the older
    `context.sources.pandas_default.read_dataframe(df)` "Validator" pattern.
    """
    # --- Register the in-memory dataframe as a Data Asset ---
    data_source = context.data_sources.add_pandas("pandas_datasource")
    data_asset = data_source.add_dataframe_asset(name="transactions")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        "transactions_batch"
    )

    # --- Build the Expectation Suite ---
    suite = context.suites.add(gx.ExpectationSuite(name="transactions_suite"))

    required_columns = [
        "transaction_id", "card_id", "merchant_name", "merchant_category",
        "transaction_amount", "transaction_timestamp",
    ]
    for col in required_columns:
        suite.add_expectation(gx.expectations.ExpectColumnToExist(column=col))

    # --- Null checks on critical fields ---
    for col in ["transaction_id", "card_id", "transaction_amount", "transaction_timestamp"]:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column=col)
        )

    # --- Uniqueness ---
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeUnique(column="transaction_id")
    )

    # --- Value range checks ---
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="transaction_amount", min_value=0.01, max_value=50000
        )
    )

    # --- Categorical domain check ---
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="merchant_category", value_set=VALID_CATEGORIES
        )
    )

    # --- Tie the suite to the batch and run it ---
    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="transactions_validation", data=batch_definition, suite=suite
        )
    )

    results = validation_definition.run(batch_parameters={"dataframe": df})
    return results


def summarize_and_save(results):
    os.makedirs(RESULTS_DIR, exist_ok=True)

    total = len(results["results"])
    passed = sum(1 for r in results["results"] if r["success"])
    failed = total - passed

    summary = {
        "run_timestamp": datetime.now().isoformat(),
        "total_expectations": total,
        "passed": passed,
        "failed": failed,
        "pass_rate": round(passed / total, 4) if total else None,
        "overall_success": results["success"],
        "failed_expectations": [
            {
                "expectation_type": r["expectation_config"]["expectation_type"],
                "column": r["expectation_config"]["kwargs"].get("column"),
                "unexpected_count": r["result"].get("unexpected_count"),
            }
            for r in results["results"] if not r["success"]
        ],
    }

    out_file = os.path.join(
        RESULTS_DIR, f"dq_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    )
    with open(out_file, "w") as f:
        json.dump(summary, f, indent=2)

    print("--- Data Quality Summary ---")
    print(f"Expectations run: {total}")
    print(f"Passed:           {passed}")
    print(f"Failed:           {failed}")
    print(f"Pass rate:        {summary['pass_rate']:.2%}" if summary['pass_rate'] is not None else "N/A")
    if summary["failed_expectations"]:
        print("\nFailed checks:")
        for f in summary["failed_expectations"]:
            print(f"  - {f['expectation_type']} on '{f['column']}' "
                  f"({f['unexpected_count']} unexpected values)")
    print(f"\nFull results written to {out_file}")

    return summary


def main():
    df = load_processed_data()
    context = gx.get_context()
    results = build_and_run_validation(context, df)
    summarize_and_save(results)


if __name__ == "__main__":
    main()
