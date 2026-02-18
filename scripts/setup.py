"""
scripts/setup.py — One-shot Snowflake bootstrap.

Runs once at docker-compose up. Idempotent (safe to re-run).

Order:
  1. DROP + CREATE database CHURN_DEMO
  2. Create all raw tables
  3. Create dynamic tables (DYN_CUSTOMER_FEATURES, DYN_CHURN_PREDICTIONS)
  4. Create stream on DYN_CHURN_PREDICTIONS
  5. Create stored procedure PROC_GENERATE_RETENTION_EMAILS
  6. Create task TASK_GENERATE_EMAILS (fires only when stream has data)
  7. Seed base data: DIM_CUSTOMERS → DIM_ACCOUNTS → FACT_TRANSACTION_LEDGER
                     → APP_ACTIVITY_LOGS → SUPPORT_CASES
"""

import sys
import os
import random
import time
from datetime import datetime, timedelta, date

import snowflake.connector
from faker import Faker

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.core.config import get_snowflake_connection_params

sys.stdout.reconfigure(encoding="utf-8")

fake = Faker()
Faker.seed(42)
random.seed(42)

# ── Seed targets ──────────────────────────────────────────────────────────────
N_CUSTOMERS    = 100_000
N_ACCOUNTS     = 200_000
N_TRANSACTIONS = 500_000
N_LOGS         = 200_000
N_SUPPORT      = 50_000
BATCH          = 10_000


# ── Helpers ───────────────────────────────────────────────────────────────────
def run(cur, sql: str, label: str = ""):
    try:
        cur.execute(sql)
        print(f"  ✅ {label or sql[:60].strip()}")
    except Exception as e:
        print(f"  ❌ {label or sql[:60].strip()}\n     {e}")


def batch_insert(conn, table: str, columns: list[str], rows: list[tuple]):
    cur = conn.cursor()
    placeholders = ", ".join(["%s"] * len(columns))
    cols = ", ".join(columns)
    sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
    cur.executemany(sql, rows)
    conn.commit()


# ── Step 1: Database ──────────────────────────────────────────────────────────
def create_database(cur):
    print("\n[1/7] Creating database CHURN_DEMO...")
    run(cur, "DROP DATABASE IF EXISTS CHURN_DEMO", "Drop old CHURN_DEMO")
    run(cur, "CREATE DATABASE CHURN_DEMO",          "Create CHURN_DEMO")
    run(cur, "USE DATABASE CHURN_DEMO",             "Use CHURN_DEMO")
    run(cur, "USE SCHEMA PUBLIC",                   "Use PUBLIC schema")


# ── Step 2: Raw tables ────────────────────────────────────────────────────────
def create_raw_tables(cur):
    print("\n[2/7] Creating raw tables...")

    run(cur, """
        CREATE OR REPLACE TABLE DIM_CUSTOMERS (
            CUSTOMER_ID         VARCHAR(20)  PRIMARY KEY,
            FULL_NAME           VARCHAR(100) NOT NULL,
            EMAIL               VARCHAR(100) UNIQUE,
            SEGMENT             VARCHAR(30),
            JOIN_DATE           DATE         NOT NULL,
            RISK_PROFILE_SCORE  FLOAT,
            CREATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """, "DIM_CUSTOMERS")

    run(cur, """
        CREATE OR REPLACE TABLE DIM_ACCOUNTS (
            ACCOUNT_ID          VARCHAR(20)  PRIMARY KEY,
            CUSTOMER_ID         VARCHAR(20)  NOT NULL REFERENCES DIM_CUSTOMERS(CUSTOMER_ID),
            PRODUCT_CODE        VARCHAR(10),
            AVAILABLE_BALANCE   FLOAT,
            ACCOUNT_STATUS      VARCHAR(10)  DEFAULT 'ACTIVE',
            OPENED_DATE         DATE
        )
    """, "DIM_ACCOUNTS")

    run(cur, """
        CREATE OR REPLACE TABLE FACT_TRANSACTION_LEDGER (
            TRANSACTION_REF         VARCHAR(20)  PRIMARY KEY,
            ACCOUNT_ID              VARCHAR(20)  NOT NULL REFERENCES DIM_ACCOUNTS(ACCOUNT_ID),
            POSTING_DATE            TIMESTAMP_NTZ NOT NULL,
            TRANSACTION_CODE        VARCHAR(50),
            AMOUNT                  FLOAT        NOT NULL,
            MERCHANT_DESCRIPTION    VARCHAR(255),
            MERCHANT_CATEGORY_CODE  VARCHAR(10),
            CHANNEL_ID              VARCHAR(20)
        )
    """, "FACT_TRANSACTION_LEDGER")

    run(cur, """
        CREATE OR REPLACE TABLE APP_ACTIVITY_LOGS (
            LOG_ID          VARCHAR(20)   PRIMARY KEY,
            CUSTOMER_ID     VARCHAR(20)   NOT NULL REFERENCES DIM_CUSTOMERS(CUSTOMER_ID),
            EVENT_TYPE      VARCHAR(50),
            EVENT_TIMESTAMP TIMESTAMP_NTZ NOT NULL,
            DEVICE_OS       VARCHAR(20),
            PAGE_URL        VARCHAR(255),
            ERROR_CODE      VARCHAR(20)
        )
    """, "APP_ACTIVITY_LOGS")

    run(cur, """
        CREATE OR REPLACE TABLE SUPPORT_CASES (
            CASE_ID         VARCHAR(20)   PRIMARY KEY,
            CUSTOMER_ID     VARCHAR(20)   NOT NULL REFERENCES DIM_CUSTOMERS(CUSTOMER_ID),
            OPEN_TIMESTAMP  TIMESTAMP_NTZ NOT NULL,
            CHANNEL         VARCHAR(20),
            CATEGORY        VARCHAR(20),
            SENTIMENT_SCORE FLOAT,
            TRANSCRIPT_TEXT TEXT
        )
    """, "SUPPORT_CASES")

    run(cur, """
        CREATE OR REPLACE TABLE AGENT_INTERVENTION_LOG (
            INTERVENTION_ID VARCHAR(36)   PRIMARY KEY,
            CUSTOMER_ID     VARCHAR(20)   NOT NULL REFERENCES DIM_CUSTOMERS(CUSTOMER_ID),
            CHURN_SCORE     FLOAT,
            GENERATED_EMAIL TEXT,
            CREATED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """, "AGENT_INTERVENTION_LOG")


# ── Step 3: Dynamic tables ────────────────────────────────────────────────────
def create_dynamic_tables(cur):
    print("\n[3/7] Creating dynamic tables...")

    run(cur, """
        CREATE OR REPLACE DYNAMIC TABLE DYN_CUSTOMER_FEATURES
            TARGET_LAG = '5 minutes'
            WAREHOUSE  = BANK_WAREHOUSE
        AS
        SELECT
            c.CUSTOMER_ID,
            COUNT(DISTINCT t.TRANSACTION_REF)                                           AS txn_count_30d,
            COALESCE(SUM(t.AMOUNT), 0)                                                  AS total_spend_30d,
            COALESCE(SUM(CASE WHEN t.TRANSACTION_CODE = 'WIRE_OUT' THEN t.AMOUNT END), 0) AS wire_out_30d,
            COUNT(DISTINCT CASE WHEN l.ERROR_CODE IS NOT NULL THEN l.LOG_ID END)        AS error_count_30d,
            COUNT(DISTINCT s.CASE_ID)                                                   AS support_cases_30d,
            COALESCE(AVG(s.SENTIMENT_SCORE), 0.5)                                       AS avg_sentiment,
            COUNT(DISTINCT DATE(l.EVENT_TIMESTAMP))                                     AS active_days_30d,
            CURRENT_TIMESTAMP()                                                         AS computed_at
        FROM DIM_CUSTOMERS c
        LEFT JOIN DIM_ACCOUNTS a
            ON c.CUSTOMER_ID = a.CUSTOMER_ID
        LEFT JOIN FACT_TRANSACTION_LEDGER t
            ON a.ACCOUNT_ID = t.ACCOUNT_ID
            AND t.POSTING_DATE >= DATEADD('day', -30, CURRENT_TIMESTAMP())
        LEFT JOIN APP_ACTIVITY_LOGS l
            ON c.CUSTOMER_ID = l.CUSTOMER_ID
            AND l.EVENT_TIMESTAMP >= DATEADD('day', -30, CURRENT_TIMESTAMP())
        LEFT JOIN SUPPORT_CASES s
            ON c.CUSTOMER_ID = s.CUSTOMER_ID
            AND s.OPEN_TIMESTAMP >= DATEADD('day', -30, CURRENT_TIMESTAMP())
        GROUP BY c.CUSTOMER_ID
    """, "DYN_CUSTOMER_FEATURES")

    run(cur, """
        CREATE OR REPLACE DYNAMIC TABLE DYN_CHURN_PREDICTIONS
            TARGET_LAG = '5 minutes'
            WAREHOUSE  = BANK_WAREHOUSE
        AS
        SELECT
            f.CUSTOMER_ID,
            c.FULL_NAME,
            c.SEGMENT,
            c.EMAIL,
            LEAST(1.0, GREATEST(0.0,
                (CASE WHEN f.wire_out_30d      > 5000 THEN 0.25 ELSE 0 END) +
                (CASE WHEN f.support_cases_30d > 2    THEN 0.20 ELSE 0 END) +
                (CASE WHEN f.avg_sentiment     < 0.4  THEN 0.20 ELSE 0 END) +
                (CASE WHEN f.error_count_30d   > 5    THEN 0.15 ELSE 0 END) +
                (CASE WHEN f.active_days_30d   < 5    THEN 0.20 ELSE 0 END)
            ))                                                          AS churn_score,
            CASE
                WHEN LEAST(1.0, GREATEST(0.0,
                    (CASE WHEN f.wire_out_30d      > 5000 THEN 0.25 ELSE 0 END) +
                    (CASE WHEN f.support_cases_30d > 2    THEN 0.20 ELSE 0 END) +
                    (CASE WHEN f.avg_sentiment     < 0.4  THEN 0.20 ELSE 0 END) +
                    (CASE WHEN f.error_count_30d   > 5    THEN 0.15 ELSE 0 END) +
                    (CASE WHEN f.active_days_30d   < 5    THEN 0.20 ELSE 0 END)
                )) >= 0.7 THEN 'HIGH'
                WHEN LEAST(1.0, GREATEST(0.0,
                    (CASE WHEN f.wire_out_30d      > 5000 THEN 0.25 ELSE 0 END) +
                    (CASE WHEN f.support_cases_30d > 2    THEN 0.20 ELSE 0 END) +
                    (CASE WHEN f.avg_sentiment     < 0.4  THEN 0.20 ELSE 0 END) +
                    (CASE WHEN f.error_count_30d   > 5    THEN 0.15 ELSE 0 END) +
                    (CASE WHEN f.active_days_30d   < 5    THEN 0.20 ELSE 0 END)
                )) >= 0.4 THEN 'MEDIUM'
                ELSE 'LOW'
            END                                                         AS risk_class,
            f.computed_at
        FROM DYN_CUSTOMER_FEATURES f
        JOIN DIM_CUSTOMERS c ON f.CUSTOMER_ID = c.CUSTOMER_ID
    """, "DYN_CHURN_PREDICTIONS")


# ── Step 4: Stream ────────────────────────────────────────────────────────────
def create_stream(cur):
    print("\n[4/7] Creating stream on DYN_CHURN_PREDICTIONS...")
    run(cur, """
        CREATE OR REPLACE STREAM STREAM_HIGH_RISK_CUSTOMERS
            ON DYNAMIC TABLE DYN_CHURN_PREDICTIONS
            SHOW_INITIAL_ROWS = FALSE
    """, "STREAM_HIGH_RISK_CUSTOMERS")


# ── Step 5: Stored procedure ──────────────────────────────────────────────────
def create_procedure(cur):
    print("\n[5/7] Creating stored procedure PROC_GENERATE_RETENTION_EMAILS...")
    run(cur, """
        CREATE OR REPLACE PROCEDURE PROC_GENERATE_RETENTION_EMAILS()
        RETURNS VARCHAR
        LANGUAGE SQL
        AS
        $$
        DECLARE
            email_count INT DEFAULT 0;
        BEGIN
            -- Only generate for HIGH risk customers not emailed in last 7 days
            -- Cap at 50 per run to control LLM cost
            INSERT INTO AGENT_INTERVENTION_LOG (
                INTERVENTION_ID, CUSTOMER_ID, CHURN_SCORE, GENERATED_EMAIL, CREATED_AT
            )
            SELECT
                UUID_STRING(),
                p.CUSTOMER_ID,
                p.churn_score,
                SNOWFLAKE.CORTEX.COMPLETE(
                    'llama3-8b',
                    CONCAT(
                        'Write a short, empathetic bank retention email (3 sentences max) for a customer named ',
                        p.FULL_NAME,
                        ' who is a ', p.SEGMENT,
                        ' customer with a churn risk score of ', ROUND(p.churn_score, 2),
                        '. Offer a relevant benefit. Sign off as BankCo Customer Success.'
                    )
                ),
                CURRENT_TIMESTAMP()
            FROM DYN_CHURN_PREDICTIONS p
            WHERE p.risk_class = 'HIGH'
              AND NOT EXISTS (
                  SELECT 1 FROM AGENT_INTERVENTION_LOG a
                  WHERE a.CUSTOMER_ID = p.CUSTOMER_ID
                    AND a.CREATED_AT > DATEADD('day', -7, CURRENT_TIMESTAMP())
              )
            LIMIT 50;

            RETURN 'Emails generated: ' || SQLROWCOUNT;
        END;
        $$
    """, "PROC_GENERATE_RETENTION_EMAILS")


# ── Step 6: Task ──────────────────────────────────────────────────────────────
def create_task(cur):
    print("\n[6/7] Creating task TASK_GENERATE_EMAILS...")
    run(cur, "ALTER TASK IF EXISTS TASK_GENERATE_EMAILS SUSPEND", "Suspend old task")
    run(cur, """
        CREATE OR REPLACE TASK TASK_GENERATE_EMAILS
            WAREHOUSE = BANK_WAREHOUSE
            SCHEDULE  = '5 MINUTES'
            WHEN SYSTEM$STREAM_HAS_DATA('CHURN_DEMO.PUBLIC.STREAM_HIGH_RISK_CUSTOMERS')
        AS
            CALL PROC_GENERATE_RETENTION_EMAILS()
    """, "TASK_GENERATE_EMAILS")
    run(cur, "ALTER TASK TASK_GENERATE_EMAILS RESUME", "Resume task")


# ── Step 7: Seed data ─────────────────────────────────────────────────────────
def seed_data(conn):
    print("\n[7/7] Seeding base data...")
    cur = conn.cursor()

    # ── Customers ──
    cur.execute("SELECT COUNT(*) FROM DIM_CUSTOMERS")
    if cur.fetchone()[0] == 0:
        print(f"  Seeding {N_CUSTOMERS:,} customers...")
        segments = ["Young Professional", "Student", "Established", "High Net Worth"]
        rows = []
        for _ in range(N_CUSTOMERS):
            cid = f"C{random.randint(10_000_000, 99_999_999)}"
            rows.append((
                cid,
                fake.name(),
                fake.unique.email(),
                random.choice(segments),
                fake.date_between(start_date="-5y", end_date="today"),
                round(random.uniform(0, 1), 2),
            ))
            if len(rows) == BATCH:
                batch_insert(conn, "DIM_CUSTOMERS",
                    ["CUSTOMER_ID","FULL_NAME","EMAIL","SEGMENT","JOIN_DATE","RISK_PROFILE_SCORE"], rows)
                rows = []
        if rows:
            batch_insert(conn, "DIM_CUSTOMERS",
                ["CUSTOMER_ID","FULL_NAME","EMAIL","SEGMENT","JOIN_DATE","RISK_PROFILE_SCORE"], rows)
        print("  ✅ Customers seeded")
    else:
        print("  ✅ Customers already exist — skipping")

    # ── Accounts ──
    cur.execute("SELECT COUNT(*) FROM DIM_ACCOUNTS")
    if cur.fetchone()[0] == 0:
        print(f"  Seeding {N_ACCOUNTS:,} accounts...")
        cur.execute("SELECT CUSTOMER_ID FROM DIM_CUSTOMERS")
        cids = [r[0] for r in cur.fetchall()]
        products = ["CHK", "SAV", "MMA", "CC"]
        rows = []
        for _ in range(N_ACCOUNTS):
            rows.append((
                f"A{random.randint(10_000_000, 99_999_999)}",
                random.choice(cids),
                random.choice(products),
                round(random.uniform(0, 50_000), 2),
                "ACTIVE",
                fake.date_between(start_date="-5y", end_date="today"),
            ))
            if len(rows) == BATCH:
                batch_insert(conn, "DIM_ACCOUNTS",
                    ["ACCOUNT_ID","CUSTOMER_ID","PRODUCT_CODE","AVAILABLE_BALANCE","ACCOUNT_STATUS","OPENED_DATE"], rows)
                rows = []
        if rows:
            batch_insert(conn, "DIM_ACCOUNTS",
                ["ACCOUNT_ID","CUSTOMER_ID","PRODUCT_CODE","AVAILABLE_BALANCE","ACCOUNT_STATUS","OPENED_DATE"], rows)
        print("  ✅ Accounts seeded")
    else:
        print("  ✅ Accounts already exist — skipping")

    # ── Transactions ──
    cur.execute("SELECT COUNT(*) FROM FACT_TRANSACTION_LEDGER")
    if cur.fetchone()[0] == 0:
        print(f"  Seeding {N_TRANSACTIONS:,} transactions...")
        cur.execute("SELECT ACCOUNT_ID FROM DIM_ACCOUNTS")
        aids = [r[0] for r in cur.fetchall()]
        tx_types = ["DEBIT_CARD_POS","ACH_CREDIT","ACH_DEBIT","WIRE_OUT","ATM_WITHDRAWAL","CHECK_DEPOSIT","FEE_OD","FEE_MONTHLY"]
        channels = ["MOBILE_APP","WEB_BANKING","BRANCH","ATM","PHONE"]
        rows = []
        for _ in range(N_TRANSACTIONS):
            tx = random.choice(tx_types)
            amt = round(random.uniform(5, 500), 2)
            if "WIRE" in tx: amt = round(random.uniform(1000, 20000), 2)
            if "FEE"  in tx: amt = round(random.uniform(5, 35), 2)
            rows.append((
                f"TX{random.randint(100_000_000, 999_999_999)}",
                random.choice(aids),
                fake.date_time_between(start_date="-90d", end_date="now"),
                tx, amt,
                fake.company()[:100] if "DEBIT" in tx else None,
                f"MCC{random.randint(1000,9999)}" if "DEBIT" in tx else None,
                random.choice(channels),
            ))
            if len(rows) == BATCH:
                batch_insert(conn, "FACT_TRANSACTION_LEDGER",
                    ["TRANSACTION_REF","ACCOUNT_ID","POSTING_DATE","TRANSACTION_CODE","AMOUNT",
                     "MERCHANT_DESCRIPTION","MERCHANT_CATEGORY_CODE","CHANNEL_ID"], rows)
                rows = []
                print(f"    {N_TRANSACTIONS - _:,} remaining...")
        if rows:
            batch_insert(conn, "FACT_TRANSACTION_LEDGER",
                ["TRANSACTION_REF","ACCOUNT_ID","POSTING_DATE","TRANSACTION_CODE","AMOUNT",
                 "MERCHANT_DESCRIPTION","MERCHANT_CATEGORY_CODE","CHANNEL_ID"], rows)
        print("  ✅ Transactions seeded")
    else:
        print("  ✅ Transactions already exist — skipping")

    # ── App Logs ──
    cur.execute("SELECT COUNT(*) FROM APP_ACTIVITY_LOGS")
    if cur.fetchone()[0] == 0:
        print(f"  Seeding {N_LOGS:,} app logs...")
        cur.execute("SELECT CUSTOMER_ID FROM DIM_CUSTOMERS SAMPLE(50000 ROWS)")
        cids = [r[0] for r in cur.fetchall()]
        events = ["LOGIN","VIEW_BALANCE","TRANSFER","ERROR","LOGOUT"]
        oses   = ["iOS","Android","Web"]
        rows = []
        for _ in range(N_LOGS):
            evt = random.choice(events)
            rows.append((
                f"LG{random.randint(10_000_000, 99_999_999)}",
                random.choice(cids),
                evt,
                fake.date_time_between(start_date="-90d", end_date="now"),
                random.choice(oses),
                "/home",
                "ERR_500" if evt == "ERROR" else None,
            ))
            if len(rows) == BATCH:
                batch_insert(conn, "APP_ACTIVITY_LOGS",
                    ["LOG_ID","CUSTOMER_ID","EVENT_TYPE","EVENT_TIMESTAMP","DEVICE_OS","PAGE_URL","ERROR_CODE"], rows)
                rows = []
        if rows:
            batch_insert(conn, "APP_ACTIVITY_LOGS",
                ["LOG_ID","CUSTOMER_ID","EVENT_TYPE","EVENT_TIMESTAMP","DEVICE_OS","PAGE_URL","ERROR_CODE"], rows)
        print("  ✅ App logs seeded")
    else:
        print("  ✅ App logs already exist — skipping")

    # ── Support Cases ──
    cur.execute("SELECT COUNT(*) FROM SUPPORT_CASES")
    if cur.fetchone()[0] == 0:
        print(f"  Seeding {N_SUPPORT:,} support cases...")
        cur.execute("SELECT CUSTOMER_ID FROM DIM_CUSTOMERS SAMPLE(20000 ROWS)")
        cids = [r[0] for r in cur.fetchall()]
        channels  = ["PHONE","EMAIL","CHAT"]
        cats      = ["BILLING","TECHNICAL","FRAUD","GENERAL"]
        rows = []
        for _ in range(N_SUPPORT):
            rows.append((
                f"CS{random.randint(1_000_000, 9_999_999)}",
                random.choice(cids),
                fake.date_time_between(start_date="-90d", end_date="now"),
                random.choice(channels),
                random.choice(cats),
                round(random.uniform(0, 1), 2),
                "Customer contacted support regarding account issue.",
            ))
            if len(rows) == BATCH:
                batch_insert(conn, "SUPPORT_CASES",
                    ["CASE_ID","CUSTOMER_ID","OPEN_TIMESTAMP","CHANNEL","CATEGORY","SENTIMENT_SCORE","TRANSCRIPT_TEXT"], rows)
                rows = []
        if rows:
            batch_insert(conn, "SUPPORT_CASES",
                ["CASE_ID","CUSTOMER_ID","OPEN_TIMESTAMP","CHANNEL","CATEGORY","SENTIMENT_SCORE","TRANSCRIPT_TEXT"], rows)
        print("  ✅ Support cases seeded")
    else:
        print("  ✅ Support cases already exist — skipping")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("🚀 CHURN INTELLIGENCE — SNOWFLAKE SETUP")
    print("=" * 60)

    params = get_snowflake_connection_params()
    # Connect without database first (for DROP/CREATE)
    root_params = {k: v for k, v in params.items() if k not in ("database", "schema")}
    conn = snowflake.connector.connect(**root_params)
    cur  = conn.cursor()

    create_database(cur)

    # Reconnect with database context
    conn.close()
    conn = snowflake.connector.connect(**params)
    cur  = conn.cursor()

    create_raw_tables(cur)
    create_dynamic_tables(cur)
    create_stream(cur)
    create_procedure(cur)
    create_task(cur)
    seed_data(conn)

    conn.close()
    print("\n" + "=" * 60)
    print("✅ SETUP COMPLETE — Pipeline is live")
    print("=" * 60)


if __name__ == "__main__":
    main()
