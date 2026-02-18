"""
streaming/producer.py — Generates realistic banking events and publishes to Kafka.

Event types (weighted):
  TXN  (70%) — banking transaction
  LOG  (25%) — app activity log
  USER  (5%) — new customer registration

Includes retry loop so it waits for Kafka to be ready (Docker startup race).
"""

import json
import time
import random
import argparse
import uuid
from datetime import datetime

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from faker import Faker

fake = Faker()
Faker.seed(0)
random.seed(0)

TOPIC = "bank_transactions"

TX_TYPES  = ["DEBIT_CARD_POS","ACH_CREDIT","ACH_DEBIT","WIRE_OUT","ATM_WITHDRAWAL","CHECK_DEPOSIT","FEE_OD"]
CHANNELS  = ["MOBILE_APP","WEB_BANKING","BRANCH","ATM","PHONE"]
EVENTS    = ["LOGIN","VIEW_BALANCE","TRANSFER","ERROR","LOGOUT"]
OSES      = ["iOS","Android","Web"]
SEGMENTS  = ["Young Professional","Student","Established","High Net Worth"]
PRODUCTS  = ["CHK","SAV","MMA","CC"]


def make_txn_event() -> dict:
    tx = random.choice(TX_TYPES)
    amt = round(random.uniform(5, 500), 2)
    if "WIRE" in tx: amt = round(random.uniform(1000, 20000), 2)
    if "FEE"  in tx: amt = round(random.uniform(5, 35), 2)
    return {
        "event_type": "TXN",
        "payload": {
            "transaction_ref":        f"TX{random.randint(100_000_000, 999_999_999)}",
            "account_id":             f"A{random.randint(10_000_000, 99_999_999)}",
            "posting_date":           datetime.utcnow().isoformat(),
            "transaction_code":       tx,
            "amount":                 amt,
            "merchant_description":   fake.company()[:100] if "DEBIT" in tx else None,
            "merchant_category_code": f"MCC{random.randint(1000,9999)}" if "DEBIT" in tx else None,
            "channel_id":             random.choice(CHANNELS),
        }
    }


def make_log_event() -> dict:
    evt = random.choice(EVENTS)
    return {
        "event_type": "LOG",
        "payload": {
            "log_id":          f"LG{random.randint(10_000_000, 99_999_999)}",
            "customer_id":     f"C{random.randint(10_000_000, 99_999_999)}",
            "event_type":      evt,
            "event_timestamp": datetime.utcnow().isoformat(),
            "device_os":       random.choice(OSES),
            "page_url":        "/home",
            "error_code":      "ERR_500" if evt == "ERROR" else None,
        }
    }


def make_user_event() -> dict:
    return {
        "event_type": "USER",
        "payload": {
            "customer_id":        f"C{random.randint(10_000_000, 99_999_999)}",
            "full_name":          fake.name(),
            "email":              fake.email(),
            "segment":            random.choice(SEGMENTS),
            "join_date":          datetime.utcnow().date().isoformat(),
            "risk_profile_score": round(random.uniform(0, 1), 2),
        }
    }


def connect_with_retry(broker: str, max_attempts: int = 30) -> KafkaProducer:
    for attempt in range(1, max_attempts + 1):
        try:
            print(f"[producer] Connecting to Kafka at {broker} (attempt {attempt}/{max_attempts})...")
            producer = KafkaProducer(
                bootstrap_servers=[broker],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                compression_type="gzip",
                acks="all",
            )
            print("[producer] ✅ Connected to Kafka")
            return producer
        except NoBrokersAvailable:
            print(f"[producer] Broker not ready — retrying in 5s...")
            time.sleep(5)
    raise RuntimeError(f"Could not connect to Kafka at {broker} after {max_attempts} attempts")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka",    default="localhost:9092")
    parser.add_argument("--duration", type=int, default=3600, help="Run duration in seconds")
    parser.add_argument("--rate",     type=int, default=200,  help="Events per minute")
    args = parser.parse_args()

    producer = connect_with_retry(args.kafka)
    interval = 60.0 / args.rate  # seconds between events
    end_time = time.time() + args.duration
    sent = 0

    print(f"[producer] Streaming {args.rate} events/min for {args.duration}s → topic '{TOPIC}'")

    try:
        while time.time() < end_time:
            roll = random.random()
            if roll < 0.70:
                event = make_txn_event()
            elif roll < 0.95:
                event = make_log_event()
            else:
                event = make_user_event()

            producer.send(TOPIC, value=event)
            sent += 1

            if sent % 500 == 0:
                print(f"[producer] Sent {sent:,} events")

            time.sleep(interval)

    except KeyboardInterrupt:
        print("[producer] Stopped by user")
    finally:
        producer.flush()
        producer.close()
        print(f"[producer] Total sent: {sent:,}")


if __name__ == "__main__":
    main()
