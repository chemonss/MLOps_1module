import json
import logging
import os
import time
from confluent_kafka import Consumer, Producer
import pandas as pd
from mlops.features import transform
from mlops.model_store import current_version, load_current

logging.basicConfig(level=logging.INFO)
consumer = Consumer({
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
    "group.id": "fraud_scorer",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})
producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")})
consumer.subscribe(["fraud_transactions"])
loaded_version, model = None, None
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
            logging.warning("No published model yet; waiting")
            continue
        if version != loaded_version:
            try:
                next_version, next_model = load_current()
                loaded_version, model = next_version, next_model
                logging.info("Loaded model %s", loaded_version)
            except Exception:
                logging.exception("Failed to load new model; retaining previous version")
                if model is None:
                    continue
        row = json.loads(message.value())
        features = transform(pd.DataFrame([row]))
        score = float(model.predict_proba(features)[0, 1])
        output = {"transaction_id": row.get("transaction_id"),
                  "score": score, "fraud_flag": int(score >= 0.5),
                  "model_version": loaded_version}
        producer.produce("fraud_scoring", json.dumps(output).encode())
        producer.flush(10)
        consumer.commit(message=message, asynchronous=False)
finally:
    consumer.close()
