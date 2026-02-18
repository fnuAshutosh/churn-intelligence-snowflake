"""
streaming/consumer.py — Reads from Kafka and micro-batch inserts into Snowflake.

Routing:
  TXN  → FACT_TRANSACTION_LEDGER
  LOG  → APP_ACTIVITY_LOGS
  USER → DIM_CUSTOMERS

Micro-batching: flush every FLUSH_SIZE messages OR every FLUSH_SECS seconds.
Retry loop: waits for Kafka to be ready before starting.
"""

import sys
import os
import json
import time

import snowflake.connector
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.core.config import get_snowflake_connection_params

TOPIC       = "bank_transactions"
FLUSH_SIZE  = 500    # flush after this many messages
FLUSH_SECS  = 5      # or after this many seconds


# ── Kafka connection ──────────────────────────────────────────────────────────
def connect_kafka(max_attempts: int = 30) -> KafkaConsumer:
    broker = os.getenv("KAFKA_BROKER", "localhost:9092")
    for attempt in range(1, max_attempts + 1):
        try:
            print(f"[consumer] Connecting to Kafka at {broker} (attempt {attempt}/{max_attempts})...")
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=[broker],
                auto_offset_reset="latest",
                group_id="churn_consumer_group",
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                consumer_timeout_ms=5000,   # don't block forever on empty topic
            )
            print("[consumer] ✅ Connected to Kafka")
            return consumer
        except (NoBrokersAvailable, Exception) as e:
            print(f"[consumer] Not ready ({e}) — retrying in 5s...")
            time.sleep(5)
    raise RuntimeError(f"Could not connect to Kafka after {max_attempts} attempts")


# ── Snowflake flush ───────────────────────────────────────────────────────────
def flush(conn, txn_buf: list, log_buf: list, user_buf: list) -> int:
    cur = conn.cursor()
    count = 0

    if txn_buf:
        cur.executemany("""
            INSERT INTO FACT_TRANSACTION_LEDGER
                (TRANSACTION_REF, ACCOUNT_ID, POSTING_DATE, TRANSACTION_CODE,
                 AMOUNT, MERCHANT_DESCRIPTION, MERCHANT_CATEGORY_CODE, CHANNEL_ID)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, txn_buf)
        count += len(txn_buf)

    if log_buf:
        cur.executemany("""
            INSERT INTO APP_ACTIVITY_LOGS
                (LOG_ID, CUSTOMER_ID, EVENT_TYPE, EVENT_TIMESTAMP,
                 DEVICE_OS, PAGE_URL, ERROR_CODE)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, log_buf)
        count += len(log_buf)

    if user_buf:
        cur.executemany("""
            INSERT INTO DIM_CUSTOMERS
                (CUSTOMER_ID, FULL_NAME, EMAIL, SEGMENT, JOIN_DATE, RISK_PROFILE_SCORE)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, user_buf)
        count += len(user_buf)

    conn.commit()
    return count


# ── Message parsing ───────────────────────────────────────────────────────────
def parse(msg: dict, txn_buf, log_buf, user_buf):
    e_type  = msg.get("event_type", "TXN")
    payload = msg.get("payload", msg)

    if e_type == "TXN":
        txn_buf.append((
            payload.get("transaction_ref"),
            payload.get("account_id"),
            payload.get("posting_date"),
            (payload.get("transaction_code") or "")[:50],
            payload.get("amount"),
            (payload.get("merchant_description") or "")[:255],
            (payload.get("merchant_category_code") or "")[:10],
            (payload.get("channel_id") or "")[:20],
        ))
    elif e_type == "LOG":
        log_buf.append((
            payload.get("log_id"),
            payload.get("customer_id"),
            payload.get("event_type"),
            payload.get("event_timestamp"),
            payload.get("device_os"),
            payload.get("page_url"),
            payload.get("error_code"),
        ))
    elif e_type == "USER":
        user_buf.append((
            payload.get("customer_id"),
            payload.get("full_name"),
            payload.get("email"),
            payload.get("segment"),
            payload.get("join_date"),
            payload.get("risk_profile_score"),
        ))


# ── Main loop ─────────────────────────────────────────────────────────────────
def main():
    print("[consumer] Starting Churn Intelligence Kafka Consumer")
    consumer = connect_kafka()
    conn     = snowflake.connector.connect(**get_snowflake_connection_params())

    txn_buf, log_buf, user_buf = [], [], []
    last_flush  = time.time()
    total       = 0

    print(f"[consumer] Listening on topic '{TOPIC}' — flush every {FLUSH_SIZE} msgs or {FLUSH_SECS}s")

    try:
        while True:
            for message in consumer:
                try:
                    parse(message.value, txn_buf, log_buf, user_buf)
                except Exception as e:
                    print(f"[consumer] ⚠️  Parse error: {e}")

                buf_size = len(txn_buf) + len(log_buf) + len(user_buf)
                elapsed  = time.time() - last_flush

                if buf_size >= FLUSH_SIZE or (elapsed >= FLUSH_SECS and buf_size > 0):
                    n = flush(conn, txn_buf, log_buf, user_buf)
                    total += n
                    print(f"[consumer] ✅ Flushed {n} rows (total: {total:,}) — "
                          f"TXN:{len(txn_buf)} LOG:{len(log_buf)} USER:{len(user_buf)}")
                    txn_buf.clear(); log_buf.clear(); user_buf.clear()
                    last_flush = time.time()

            # consumer_timeout_ms hit — flush any remaining
            buf_size = len(txn_buf) + len(log_buf) + len(user_buf)
            if buf_size > 0:
                n = flush(conn, txn_buf, log_buf, user_buf)
                total += n
                print(f"[consumer] ✅ Timeout flush {n} rows (total: {total:,})")
                txn_buf.clear(); log_buf.clear(); user_buf.clear()
                last_flush = time.time()

    except KeyboardInterrupt:
        print("[consumer] Stopped by user")
    except Exception as e:
        print(f"[consumer] ❌ Fatal error: {e}")
        raise
    finally:
        # Final flush
        buf_size = len(txn_buf) + len(log_buf) + len(user_buf)
        if buf_size > 0:
            flush(conn, txn_buf, log_buf, user_buf)
        consumer.close()
        conn.close()
        print(f"[consumer] Closed. Total rows inserted: {total:,}")


if __name__ == "__main__":
    main()
