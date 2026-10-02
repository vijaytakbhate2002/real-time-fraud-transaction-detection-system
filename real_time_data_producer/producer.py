import argparse
import csv
import json
import sys
import time
from pathlib import Path

from kafka import KafkaProducer

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import RAW_INPUT
START_INDEX = 311360


def parse_args(path:Path):
    parser = argparse.ArgumentParser(
        description="Replay each row of a CSV dataset to Kafka."
    )
    parser.add_argument("dataset", 
                        type=Path, 
                        help="Path to CSV dataset (default: data/raw_input/credit_card_transactions.csv)", 
                        nargs="?",
                        default=path
                        )
    parser.add_argument(
        "--interval",
        type=float,
        default=1,
        help="Seconds to wait between rows (default: 0.01)",
    )
    parser.add_argument(
        "--topic",
        default="transaction-stream",
        help="Kafka topic (default: transaction-stream)",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    return parser.parse_args()


def main():

    args = parse_args(RAW_INPUT)
    print("Starting Kafka producer with the following parameters:")
    print(f"  Dataset: {args.dataset}")
    if args.interval < 0:
        raise SystemExit("--interval must be zero or greater")
    if not args.dataset.is_file():
        raise SystemExit(f"Dataset not found: {args.dataset}")

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    sent_count = 0
    try:
        with args.dataset.open("r", newline="", encoding="utf-8-sig") as dataset:
            rows = csv.DictReader(dataset)
            if rows.fieldnames is None:
                raise SystemExit("Dataset is empty or missing a CSV header")

            for row_index, row in enumerate(rows):
                if row_index < START_INDEX:
                    continue
                if sent_count and args.interval:
                    time.sleep(args.interval)
                producer.send(args.topic, value=row)
                sent_count += 1

        producer.flush()
    finally:
        producer.close()

    print(f"Published {sent_count} rows from {args.dataset} to '{args.topic}'.")


if __name__ == "__main__":
    main()
