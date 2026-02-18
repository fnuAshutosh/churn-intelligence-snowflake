"""One-shot patch: fix stream name in TASK_GENERATE_EMAILS."""
import sys, os
sys.path.insert(0, '.')
from src.core.config import get_snowflake_connection_params
import snowflake.connector

conn = snowflake.connector.connect(**get_snowflake_connection_params())
cur  = conn.cursor()

cur.execute("ALTER TASK CHURN_DEMO.PUBLIC.TASK_GENERATE_EMAILS SUSPEND")
print("Task suspended")

cur.execute("""
    CREATE OR REPLACE TASK CHURN_DEMO.PUBLIC.TASK_GENERATE_EMAILS
        WAREHOUSE = BANK_WAREHOUSE
        SCHEDULE  = '5 MINUTES'
        WHEN SYSTEM$STREAM_HAS_DATA('CHURN_DEMO.PUBLIC.STREAM_NEW_TRANSACTIONS')
    AS
        CALL CHURN_DEMO.PUBLIC.PROC_GENERATE_RETENTION_EMAILS()
""")
print("Task recreated with correct stream name")

cur.execute("ALTER TASK CHURN_DEMO.PUBLIC.TASK_GENERATE_EMAILS RESUME")
print("Task resumed")

conn.close()
print("Patch complete")
