"""
scripts/deploy_cortex.py — Deploys Cortex Search Service and uploads semantic model.

Runs once after setup.py completes.
"""

import sys
import os

import snowflake.connector

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.core.config import get_snowflake_connection_params

sys.stdout.reconfigure(encoding="utf-8")

SEMANTIC_MODEL_PATH = os.path.join(os.path.dirname(__file__), "..", "semantic_model.yaml")


def run(cur, sql: str, label: str = ""):
    try:
        cur.execute(sql)
        print(f"  ✅ {label or sql[:60].strip()}")
    except Exception as e:
        print(f"  ❌ {label or sql[:60].strip()}\n     {e}")


def main():
    print("=" * 60)
    print("🧠 DEPLOYING CORTEX RESOURCES")
    print("=" * 60)

    params = get_snowflake_connection_params()
    conn = snowflake.connector.connect(**params)
    cur  = conn.cursor()

    # 1. Internal stage for semantic model
    print("\n[1/3] Creating internal stage AGENT_ASSETS...")
    run(cur, "CREATE STAGE IF NOT EXISTS AGENT_ASSETS", "Stage AGENT_ASSETS")

    # 2. Upload semantic model YAML
    print("\n[2/3] Uploading semantic_model.yaml...")
    abs_path = os.path.abspath(SEMANTIC_MODEL_PATH)
    if os.path.exists(abs_path):
        # Snowflake PUT requires forward slashes
        put_path = abs_path.replace("\\", "/")
        run(cur, f"PUT 'file://{put_path}' @AGENT_ASSETS OVERWRITE=TRUE AUTO_COMPRESS=FALSE",
            "Upload semantic_model.yaml")
    else:
        print(f"  ⚠️  semantic_model.yaml not found at {abs_path} — skipping upload")

    # 3. Cortex Search Service on APP_ACTIVITY_LOGS (error logs only)
    print("\n[3/3] Creating Cortex Search Service CHURN_LOGS_SEARCH...")
    run(cur, """
        CREATE OR REPLACE CORTEX SEARCH SERVICE CHURN_LOGS_SEARCH
            ON ERROR_CODE, EVENT_TYPE, PAGE_URL, DEVICE_OS
            ATTRIBUTES CUSTOMER_ID, EVENT_TIMESTAMP
            WAREHOUSE = BANK_WAREHOUSE
            TARGET_LAG = '1 hour'
        AS
            SELECT
                LOG_ID,
                CUSTOMER_ID,
                EVENT_TYPE,
                EVENT_TIMESTAMP,
                DEVICE_OS,
                PAGE_URL,
                ERROR_CODE
            FROM APP_ACTIVITY_LOGS
            WHERE ERROR_CODE IS NOT NULL
    """, "CHURN_LOGS_SEARCH")

    conn.close()
    print("\n" + "=" * 60)
    print("✅ CORTEX RESOURCES DEPLOYED")
    print("=" * 60)


if __name__ == "__main__":
    main()
