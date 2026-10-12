import argparse
import csv
import json
import os

from confluent_kafka import Producer


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("--csv", default="data/test.csv")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "fraud_transactions"))
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": args.bootstrap})
    sent = 0

    with open(args.csv, encoding="utf-8-sig", newline="") as file:
        for row in csv.DictReader(file):
            if args.limit and sent >= args.limit:
                break

            message = json.dumps(row, ensure_ascii=False).encode("utf-8")
            producer.produce(args.topic, value=message)
            producer.poll(0)
            sent += 1

    remaining = producer.flush()
    if remaining:
        raise RuntimeError(f"Не удалось завершить отправку {remaining} сообщений")
    print(f"Отправлено в Kafka: {sent} сообщений")


if __name__ == "__main__":
    main()