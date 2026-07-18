"""
Generates synthetic credit card transaction data for the pipeline.

Intentionally injects realistic data-quality problems (nulls, duplicate
transaction IDs, out-of-range amounts, bad timestamps) so the downstream
Great Expectations suite has real issues to catch -- this is what makes the
project a believable demonstration of a data-quality framework rather than
just an ETL exercise.

Output: data/raw/transactions_raw.csv
"""

import csv
import os
import random
import uuid
from datetime import datetime, timedelta

from faker import Faker

fake = Faker()
random.seed(42)
Faker.seed(42)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "raw")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "transactions_raw.csv")

NUM_RECORDS = 50_000
NUM_CARDS = 2_000

MERCHANT_CATEGORIES = [
    "grocery", "electronics", "travel", "restaurant", "gas_station",
    "online_retail", "utilities", "entertainment", "pharmacy", "jewelry",
]

# Categories flagged as inherently higher fraud-risk for the demo dashboard
HIGH_RISK_CATEGORIES = {"electronics", "jewelry", "online_retail"}


def random_timestamp(days_back: int = 90) -> datetime:
    start = datetime.now() - timedelta(days=days_back)
    return start + timedelta(
        seconds=random.randint(0, days_back * 24 * 60 * 60)
    )


def generate_card_ids(n: int) -> list:
    return [f"CARD-{uuid.uuid4().hex[:10].upper()}" for _ in range(n)]


def generate_transactions(num_records: int, card_ids: list) -> list:
    rows = []

    for i in range(num_records):
        card_id = random.choice(card_ids)
        category = random.choice(MERCHANT_CATEGORIES)
        is_high_risk = category in HIGH_RISK_CATEGORIES

        # base amount, skewed higher for high-risk categories
        amount = round(random.uniform(5, 400) * (2.5 if is_high_risk else 1), 2)

        # fraud label -- rare event, more likely in high-risk categories
        fraud_prob = 0.03 if is_high_risk else 0.005
        is_fraud = random.random() < fraud_prob

        row = {
            "transaction_id": f"TXN-{uuid.uuid4().hex[:12].upper()}",
            "card_id": card_id,
            "merchant_name": fake.company(),
            "merchant_category": category,
            "transaction_amount": amount,
            "transaction_timestamp": random_timestamp().isoformat(),
            "merchant_city": fake.city(),
            "merchant_state": fake.state_abbr(),
            "is_fraud": int(is_fraud),
        }
        rows.append(row)

    # --- Inject data-quality issues on purpose ---

    # 1. Null merchant_category (~1% of rows)
    for row in random.sample(rows, k=int(num_records * 0.01)):
        row["merchant_category"] = ""

    # 2. Null transaction_amount (~0.5% of rows)
    for row in random.sample(rows, k=int(num_records * 0.005)):
        row["transaction_amount"] = ""

    # 3. Negative / clearly invalid amounts (~0.3% of rows)
    for row in random.sample(rows, k=int(num_records * 0.003)):
        row["transaction_amount"] = round(random.uniform(-500, -1), 2)

    # 4. Duplicate transaction_id (~0.8% of rows duplicated verbatim)
    duplicates = [dict(r) for r in random.sample(rows, k=int(num_records * 0.008))]
    rows.extend(duplicates)

    # 5. Malformed timestamp (~0.2% of rows)
    for row in random.sample(rows, k=int(num_records * 0.002)):
        row["transaction_timestamp"] = "not-a-timestamp"

    random.shuffle(rows)
    return rows


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    card_ids = generate_card_ids(NUM_CARDS)
    rows = generate_transactions(NUM_RECORDS, card_ids)

    fieldnames = [
        "transaction_id", "card_id", "merchant_name", "merchant_category",
        "transaction_amount", "transaction_timestamp", "merchant_city",
        "merchant_state", "is_fraud",
    ]

    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Generated {len(rows)} records ({num_dupes(rows)} intentional duplicates)")
    print(f"Written to {OUTPUT_FILE}")


def num_dupes(rows):
    seen, dupes = set(), 0
    for r in rows:
        tid = r["transaction_id"]
        if tid in seen:
            dupes += 1
        seen.add(tid)
    return dupes


if __name__ == "__main__":
    main()
