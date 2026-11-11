
import json
import logging
import os
import time

import pandas as pd
from confluent_kafka import Consumer, Producer
from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    start_http_server,
)

from mlops.features import transform
from mlops.model_store import current_version, load_current


logging.basicConfig(level=logging.INFO)

# Prometheus metrics

PROCESSED = Counter(
    "fraud_scoring_processed_total",
    "Total number of scored transactions",
)

DURATION = Histogram(
    "fraud_scoring_duration_seconds",
    "Duration of transaction scoring in seconds",
)

SCORE = Histogram(
    "fraud_scoring_score",
    "Distribution of fraud scores",
    buckets=(
        0.0, 0.1, 0.2, 0.3, 0.4, 0.5,
        0.6, 0.7, 0.8, 0.9, 1.0,
    ),
)

FRAUD = Counter(
    "fraud_scoring_fraud_total",
    "Total number of transactions flagged as fraud",
)

MODEL_RELOAD = Counter(
    "fraud_model_reload_total",
    "Number of model loads by version",
    ["version"],
)

MODEL_RELOAD_TIME = Gauge(
    "fraud_model_reload_timestamp_seconds",
    "Timestamp when a model version was loaded",
    ["version"],
)

LAST_PROCESSED = Gauge(
    "fraud_scoring_last_processed_timestamp_seconds",
    "Timestamp of the last scored transaction",
)


def main():
    start_http_server(8000)
    logging.info("Scoring metrics available on port 8000")

    consumer = Consumer({
        "bootstrap.servers": os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"
        ),
        "group.id": "fraud_scorer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    producer = Producer({
        "bootstrap.servers": os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"
        )
    })

    consumer.subscribe(["fraud_transactions"])

    loaded_version = None
    model = None

    try:
        while True:
            if model is None and current_version() is None:
                time.sleep(2)
                continue

            message = consumer.poll(1)

            if message is None:
                continue

            if message.error():
                raise RuntimeError(message.error())

            version = current_version()

            if version is None:
                logging.warning("No published model yet")
                continue

            if version != loaded_version:
                try:
                    next_version, next_model = load_current()

                    loaded_version = next_version
                    model = next_model

                    MODEL_RELOAD.labels(
                        version=str(loaded_version)
                    ).inc()

                    MODEL_RELOAD_TIME.labels(
                        version=str(loaded_version)
                    ).set(time.time())

                    logging.info(
                        "Loaded model %s", loaded_version
                    )

                except Exception:
                    logging.exception(
                        "Failed to load model; retaining previous version"
                    )

                    if model is None:
                        continue

            row = json.loads(message.value())

            start = time.perf_counter()

            features = transform(pd.DataFrame([row]))
            score = float(
                model.predict_proba(features)[0, 1]
            )

            duration = time.perf_counter() - start
            fraud_flag = int(score >= 0.5)

            output = {
                "transaction_id": row.get("transaction_id"),
                "score": score,
                "fraud_flag": fraud_flag,
                "model_version": loaded_version,
                "us_state": row.get("us_state", row.get("state")),
                "merch": row.get("merch"),
                "cat_id": row.get("cat_id"),
            }

            producer.produce(
                "fraud_scoring",
                json.dumps(output).encode("utf-8"),
            )

            remaining = producer.flush(10)

            if remaining:
                raise RuntimeError(
                    "Failed to deliver scoring result to Kafka"
                )

            consumer.commit(
                message=message,
                asynchronous=False,
            )

            PROCESSED.inc()
            DURATION.observe(duration)
            SCORE.observe(score)
            FRAUD.inc(fraud_flag)
            LAST_PROCESSED.set(time.time())

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
