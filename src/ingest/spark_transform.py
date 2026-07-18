"""
PySpark transformation job.

Reads the raw synthetic transactions CSV, cleans and deduplicates it, and
engineers a few features that are typically useful for card fraud /
spend-analytics work:

  - transaction_hour, transaction_day_of_week (time-based features)
  - rolling 24h transaction count per card (spend velocity proxy)
  - is_high_risk_category flag

Writes cleaned output to data/processed/ as partitioned Parquet.

Run: python src/ingest/spark_transform.py
"""

import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

RAW_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw", "transactions_raw.csv")
PROCESSED_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "data", "processed")

HIGH_RISK_CATEGORIES = ["electronics", "jewelry", "online_retail"]


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("credit-card-transaction-transform")
        .master("local[*]")
        # Disable ANSI mode so malformed casts (e.g. bad timestamps injected
        # by the synthetic data generator) become null instead of raising --
        # we want to catch and count these as a data-quality issue, not crash.
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )


def load_raw(spark: SparkSession):
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(RAW_PATH)
    )


def clean_and_transform(df):
    # Cast amount to double; malformed values become null, which we then
    # filter out and count -- this is exactly the kind of issue the Great
    # Expectations suite in src/quality/ also checks for independently.
    df = df.withColumn(
        "transaction_amount", F.col("transaction_amount").cast(DoubleType())
    )

    df = df.withColumn(
        "transaction_timestamp",
        F.to_timestamp("transaction_timestamp"),
    )

    total_rows = df.count()

    # Drop rows with unusable core fields
    df_clean = df.filter(
        F.col("transaction_amount").isNotNull()
        & (F.col("transaction_amount") > 0)
        & F.col("transaction_timestamp").isNotNull()
        & F.col("merchant_category").isNotNull()
        & (F.col("merchant_category") != "")
    )

    dropped_bad_rows = total_rows - df_clean.count()

    # Deduplicate on transaction_id, keeping the first occurrence
    window = Window.partitionBy("transaction_id").orderBy(F.lit(1))
    df_dedup = (
        df_clean.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    dropped_duplicates = df_clean.count() - df_dedup.count()

    # --- Feature engineering ---
    df_features = (
        df_dedup
        .withColumn("transaction_hour", F.hour("transaction_timestamp"))
        .withColumn("transaction_day_of_week", F.date_format("transaction_timestamp", "EEEE"))
        .withColumn(
            "is_high_risk_category",
            F.col("merchant_category").isin(HIGH_RISK_CATEGORIES),
        )
    )

    # Rolling 24h transaction count per card (spend velocity proxy).
    # True streaming velocity needs a time-range window; here we approximate
    # with a per-card, per-day count, which is sufficient for a batch demo.
    df_features = df_features.withColumn(
        "transaction_date", F.to_date("transaction_timestamp")
    )
    velocity_window = Window.partitionBy("card_id", "transaction_date")
    df_features = df_features.withColumn(
        "card_daily_txn_count", F.count("transaction_id").over(velocity_window)
    )

    print("--- Transform summary ---")
    print(f"Total raw rows:            {total_rows}")
    print(f"Dropped (nulls/invalid):   {dropped_bad_rows}")
    print(f"Dropped (duplicates):      {dropped_duplicates}")
    print(f"Final clean row count:     {df_features.count()}")

    return df_features


def main():
    spark = get_spark()
    raw_df = load_raw(spark)
    transformed_df = clean_and_transform(raw_df)

    (
        transformed_df
        .write
        .mode("overwrite")
        .partitionBy("transaction_date")
        .parquet(PROCESSED_PATH)
    )

    print(f"Wrote cleaned data to {PROCESSED_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()
