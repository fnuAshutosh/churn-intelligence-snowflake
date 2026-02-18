"""
scripts/deploy_cortex.py — Deploys all Cortex AI resources.

Steps:
  1. Internal stage AGENT_ASSETS (for semantic model YAML)
  2. Upload semantic_model.yaml to stage
  3. Cortex Search Service on APP_ACTIVITY_LOGS
  4. Cortex Analyst semantic view (SQL view that Analyst uses)
  5. Cortex Agent (REST endpoint wrapping Search + Analyst)
  6. Push Streamlit app to Snowflake (SiS)
"""

import sys
import os

import snowflake.connector

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.core.config import get_snowflake_connection_params

SEMANTIC_MODEL_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "semantic_model.yaml")
)
DASHBOARD_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "src", "app", "dashboard.py")
)


def run(cur, sql: str, label: str = "", fatal: bool = False):
    try:
        cur.execute(sql)
        print(f"  ✅ {label or sql[:70].strip()}")
        return True
    except Exception as e:
        print(f"  ❌ {label or sql[:70].strip()}\n     {e}")
        if fatal:
            raise
        return False


def main():
    print("=" * 60)
    print("🧠 DEPLOYING CORTEX RESOURCES")
    print("=" * 60)

    params = get_snowflake_connection_params()
    conn = snowflake.connector.connect(**params)
    cur  = conn.cursor()

    # Ensure we're in the right context
    cur.execute(f"USE DATABASE {params['database']}")
    cur.execute(f"USE SCHEMA {params['schema']}")
    cur.execute(f"USE WAREHOUSE {params['warehouse']}")
    print(f"  Context: {params['database']}.{params['schema']} @ {params['warehouse']}")

    # ── 1. Internal stage ─────────────────────────────────────────────────────
    print("\n[1/6] Creating internal stage AGENT_ASSETS...")
    run(cur, "CREATE STAGE IF NOT EXISTS AGENT_ASSETS DIRECTORY = (ENABLE = TRUE)",
        "Stage AGENT_ASSETS")

    # ── 2. Upload semantic model ───────────────────────────────────────────────
    print("\n[2/6] Uploading semantic_model.yaml...")
    if os.path.exists(SEMANTIC_MODEL_PATH):
        # In Docker on Linux, path is already forward-slash
        put_path = SEMANTIC_MODEL_PATH.replace("\\", "/")
        run(cur,
            f"PUT 'file://{put_path}' @AGENT_ASSETS OVERWRITE=TRUE AUTO_COMPRESS=FALSE",
            "Upload semantic_model.yaml")
    else:
        print(f"  ⚠️  Not found: {SEMANTIC_MODEL_PATH}")

    # ── 3. Cortex Search Service ───────────────────────────────────────────────
    print("\n[3/6] Creating Cortex Search Service CHURN_LOGS_SEARCH...")
    run(cur, """
        CREATE OR REPLACE CORTEX SEARCH SERVICE CHURN_LOGS_SEARCH
            ON ERROR_CODE
            ATTRIBUTES CUSTOMER_ID, EVENT_TYPE, DEVICE_OS, PAGE_URL, EVENT_TIMESTAMP
            WAREHOUSE = BANK_WAREHOUSE
            TARGET_LAG = '1 hour'
        AS
            SELECT
                LOG_ID,
                CUSTOMER_ID,
                EVENT_TYPE,
                CAST(EVENT_TIMESTAMP AS VARCHAR)  AS EVENT_TIMESTAMP,
                DEVICE_OS,
                PAGE_URL,
                ERROR_CODE
            FROM APP_ACTIVITY_LOGS
            WHERE ERROR_CODE IS NOT NULL
    """, "CHURN_LOGS_SEARCH")

    # ── 4. Cortex Analyst semantic view ───────────────────────────────────────
    # A SQL view that Cortex Analyst can query via the semantic model.
    # This flattens the join so Analyst has a single denormalised surface.
    print("\n[4/6] Creating Cortex Analyst semantic view ANALYST_CHURN_VIEW...")
    run(cur, """
        CREATE OR REPLACE VIEW ANALYST_CHURN_VIEW AS
        SELECT
            p.CUSTOMER_ID,
            p.FULL_NAME,
            p.SEGMENT,
            p.EMAIL,
            ROUND(p.CHURN_SCORE, 3)          AS CHURN_SCORE,
            p.RISK_CLASS,
            p.COMPUTED_AT,
            f.TXN_COUNT_30D,
            f.TOTAL_SPEND_30D,
            f.WIRE_OUT_30D,
            f.ERROR_COUNT_30D,
            f.SUPPORT_CASES_30D,
            f.AVG_SENTIMENT,
            f.ACTIVE_DAYS_30D,
            CASE WHEN a.CUSTOMER_ID IS NOT NULL THEN TRUE ELSE FALSE END AS EMAIL_SENT_7D,
            a.CREATED_AT                     AS EMAIL_SENT_AT
        FROM DYN_CHURN_PREDICTIONS p
        JOIN DYN_CUSTOMER_FEATURES f ON p.CUSTOMER_ID = f.CUSTOMER_ID
        LEFT JOIN (
            SELECT CUSTOMER_ID, MAX(CREATED_AT) AS CREATED_AT
            FROM AGENT_INTERVENTION_LOG
            WHERE CREATED_AT > DATEADD('day', -7, CURRENT_TIMESTAMP())
            GROUP BY CUSTOMER_ID
        ) a ON p.CUSTOMER_ID = a.CUSTOMER_ID
    """, "ANALYST_CHURN_VIEW")

    # ── 5. Cortex Agent ────────────────────────────────────────────────────────
    # Cortex Agent wraps both Search and Analyst into a single conversational
    # endpoint. Requires CORTEX_ANALYST_SNOWFLAKE_CORTEX_USER privilege.
    print("\n[5/6] Creating Cortex Agent CHURN_INTELLIGENCE_AGENT...")
    run(cur, """
        CREATE OR REPLACE CORTEX AGENT CHURN_INTELLIGENCE_AGENT
        AS
        $$
        {
          "description": "BankCo Churn Intelligence Agent — answers questions about customer churn risk, transaction patterns, and AI-generated retention emails.",
          "tools": [
            {
              "tool_type": "cortex_analyst_text_to_sql",
              "tool_name": "churn_analyst",
              "tool_spec": {
                "semantic_model_file": "@AGENT_ASSETS/semantic_model.yaml"
              }
            },
            {
              "tool_type": "cortex_search",
              "tool_name": "log_search",
              "tool_spec": {
                "service_name": "CHURN_LOGS_SEARCH",
                "max_results": 10
              }
            }
          ],
          "tool_choice": "auto"
        }
        $$
    """, "CHURN_INTELLIGENCE_AGENT")

    # ── 6. Push Streamlit app to Snowflake ────────────────────────────────────
    print("\n[6/6] Pushing Streamlit app to Snowflake (SiS)...")
    if os.path.exists(DASHBOARD_PATH):
        # Upload dashboard.py to stage first
        put_path = DASHBOARD_PATH.replace("\\", "/")
        run(cur,
            f"PUT 'file://{put_path}' @AGENT_ASSETS OVERWRITE=TRUE AUTO_COMPRESS=FALSE",
            "Upload dashboard.py to stage")

        # Create the Streamlit app object in Snowflake
        run(cur, """
            CREATE OR REPLACE STREAMLIT CHURN_DASHBOARD
                ROOT_LOCATION = '@AGENT_ASSETS'
                MAIN_FILE = 'dashboard.py'
                QUERY_WAREHOUSE = BANK_WAREHOUSE
                TITLE = 'BankCo Churn Intelligence'
                COMMENT = 'Real-time churn risk dashboard powered by Dynamic Tables + Cortex AI'
        """, "STREAMLIT CHURN_DASHBOARD")

        # Get the URL
        try:
            cur.execute("SHOW STREAMLITS LIKE 'CHURN_DASHBOARD'")
            rows = cur.fetchall()
            if rows:
                print(f"\n  🌐 Dashboard URL: Open Snowflake UI → Streamlit → CHURN_DASHBOARD")
        except Exception:
            pass
    else:
        print(f"  ⚠️  dashboard.py not found at {DASHBOARD_PATH}")

    conn.close()
    print("\n" + "=" * 60)
    print("✅ ALL CORTEX RESOURCES DEPLOYED")
    print("=" * 60)
    print("""
  Resources created:
    ❄️  Stage:          AGENT_ASSETS
    🔍  Cortex Search:  CHURN_LOGS_SEARCH
    📊  Analyst View:   ANALYST_CHURN_VIEW
    🤖  Cortex Agent:   CHURN_INTELLIGENCE_AGENT
    🌐  Streamlit:      CHURN_DASHBOARD
""")


if __name__ == "__main__":
    main()
