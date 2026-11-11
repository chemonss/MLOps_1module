import json
import logging
import os
import time

import psycopg2
from confluent_kafka import Consumer
from prometheus_client import Counter, Gauge, Histogram, start_http_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

WRITTEN = Counter(
    "fraud_writer_written_total",
    "Number of successfully inserted scoring results",
)
WRITE_SECONDS = Histogram(
    "fraud_writer_write_seconds",
    "PostgreSQL write duration in seconds",
)
FRAUD_WRITTEN = Counter(
    "fraud_writer_fraud_total",
    "Number of inserted transactions flagged as fraud",
)
SCORE_HISTOGRAM = Histogram(
    "fraud_writer_score",
    "Distribution of persisted fraud scores",
    buckets=(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0),
)
LAST_WRITE = Gauge(
    "fraud_writer_last_write_timestamp_seconds",
    "Unix timestamp of the last successful insert",
)


def connect_postgres():
    while True:
        try:
            return psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "postgres"),
                dbname=os.getenv("POSTGRES_DB", "fraud_monitoring"),
                user=os.getenv("POSTGRES_USER", "fraud"),
                password=os.environ["POSTGRES_PASSWORD"],
                connect_timeout=5,
            )
        except psycopg2.OperationalError:
            logging.exception("PostgreSQL unavailable; retrying")
            time.sleep(3)


def main():
    start_http_server(8000)
    logging.info("Prometheus metrics available on port 8000")

    db = connect_postgres()

    consumer = Consumer({
        "bootstrap.servers": os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"
        ),
        "group.id": "fraud_results_writer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    consumer.subscribe(["fraud_scoring"])
    logging.info("Listening to Kafka topic fraud_scoring")

    try:
        while True:
            message = consumer.poll(1)

            if message is None:
                continue

            if message.error():
                logging.error("Kafka error: %s", message.error())
                continue

            try:
                result = json.loads(message.value())
                transaction_id = str(result["transaction_id"])
                score = float(result["score"])
                fraud_flag = int(result["fraud_flag"])
                model_version = str(result["model_version"])

                with WRITE_SECONDS.time():
                    with db:
                        with db.cursor() as cursor:
                            cursor.execute(
                                """
                                INSERT INTO scoring_results (
                                    transaction_id,
                                    score,
                                    fraud_flag,
                                    model_version,
                                    us_state,
                                    merch,
                                    cat_id
                                )
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (
                                    transaction_id, model_version
                                ) DO NOTHING
                                RETURNING id
                                """,
                                (
                                    transaction_id,
                                    score,
                                    fraud_flag,
                                    model_version,
                                    result.get("us_state"),
                                    result.get("merch"),
                                    result.get("cat_id"),
                                ),
                            )
                            inserted = cursor.fetchone() is not None

                if inserted:
                    WRITTEN.inc()
                    FRAUD_WRITTEN.inc(fraud_flag)
                    SCORE_HISTOGRAM.observe(score)
                    LAST_WRITE.set(time.time())

                consumer.commit(message=message, asynchronous=False)

            except Exception:
                db.rollback()
                logging.exception(
                    "Failed to process message; retrying after reconnect"
                )
                try:
                    db.close()
                except Exception:
                    pass
                db = connect_postgres()
                time.sleep(2)

    finally:
        consumer.close()
        db.close()


if __name__ == "__main__":
    main()
