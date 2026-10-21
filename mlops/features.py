import numpy as np
import pandas as pd

CATEGORICAL = ["merch", "cat_id", "gender", "us_state"]
NUMERIC = ["amount", "lat", "lon", "merchant_lat", "merchant_lon",
           "population_city", "hour", "weekday", "distance", "log_amount"]
FEATURES = CATEGORICAL + NUMERIC

def transform(frame):
    """Build identical training/inference features without using target leakage."""
    df = frame.copy()
    time = pd.to_datetime(df["transaction_time"], errors="coerce")
    df["hour"] = time.dt.hour.fillna(0)
    df["weekday"] = time.dt.dayofweek.fillna(0)
    for col in ["amount", "lat", "lon", "merchant_lat", "merchant_lon", "population_city"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    df["distance"] = np.hypot(df["lat"] - df["merchant_lat"],
                              df["lon"] - df["merchant_lon"])
    df["log_amount"] = np.log1p(df["amount"].clip(lower=0))
    for col in CATEGORICAL:
        df[col] = df[col].fillna("unknown").astype(str).replace("", "unknown")
    return df[FEATURES]
