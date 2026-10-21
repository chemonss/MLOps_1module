"""Load a labeled CSV (target -> is_fraud) into ClickHouse.

Usage: docker compose run --rm ml-cli scripts/load_labeled_csv.py --csv data/train.csv
"""
import argparse
import pandas as pd
from mlops.db import client
from datetime import date

COLUMNS = ["transaction_time", "merch", "cat_id", "amount", "name_1", "name_2",
           "gender", "street", "one_city", "us_state", "post_code", "lat", "lon",
           "population_city", "jobs", "merchant_lat", "merchant_lon", "is_fraud"]
ALIASES = {
    "trans_date_trans_time": "transaction_time", "merchant": "merch",
    "category": "cat_id", "amt": "amount", "first": "name_1",
    "last": "name_2", "city": "one_city", "state": "us_state",
    "zip": "post_code", "city_pop": "population_city", "job": "jobs",
    "merch_lat": "merchant_lat", "merch_long": "merchant_lon",
    "long": "lon", "target": "is_fraud"
}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default="data/train.csv")
    parser.add_argument("--batch", type=int, default=20000)
    args = parser.parse_args()
    c = client()
    # Repeatable bootstrap: replace the seed rather than append duplicates.
    c.command("TRUNCATE TABLE fraud.labeled_transactions")
    c.command("TRUNCATE TABLE fraud.training_state")
    total = 0
    months = set()
    for chunk in pd.read_csv(args.csv, chunksize=args.batch, low_memory=False):
        chunk = chunk.rename(columns=ALIASES)
        missing = set(COLUMNS) - set(chunk.columns)
        if missing:
            raise ValueError(f"Missing required columns: {sorted(missing)}")
        chunk = chunk[COLUMNS].copy()
        chunk["transaction_time"] = pd.to_datetime(chunk["transaction_time"], errors="raise")
        chunk["is_fraud"] = pd.to_numeric(chunk["is_fraud"], errors="raise").astype("int64")
        if not chunk["is_fraud"].isin([0, 1]).all():
            raise ValueError("target must contain only 0/1")
        for col in ["amount", "lat", "lon", "merchant_lat", "merchant_lon", "population_city"]:
            chunk[col] = pd.to_numeric(chunk[col], errors="raise")
        for col in ["merch", "cat_id", "name_1", "name_2", "gender", "street",
                    "one_city", "us_state", "post_code", "jobs"]:
            chunk[col] = chunk[col].fillna("").astype(str)
        months.update(chunk["transaction_time"].dt.to_period("M").astype(str).unique())
        c.insert("fraud.labeled_transactions", list(chunk.itertuples(index=False, name=None)), column_names=COLUMNS)
        total += len(chunk)
        print(f"Loaded {total} rows", flush=True)
    if len(months) < 3:
        raise ValueError("At least three calendar months are required")
    first_three = sorted(months)[:3]
    if [str(x) for x in pd.period_range(first_three[0], periods=3, freq="M")] != first_three:
        raise ValueError("First three months must be consecutive")
    initial_month = date.fromisoformat(first_three[2] + "-01")
    c.insert(
        "fraud.training_state",
        [[1, initial_month]],
        column_names=["id", "next_month"]
    )
    print("Training window ends:", first_three[2])

if __name__ == "__main__":
    main()
