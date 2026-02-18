import sys
sys.path.insert(0, '.')
from src.core.config import get_snowflake_connection_params
import snowflake.connector

conn = snowflake.connector.connect(**get_snowflake_connection_params())
cur = conn.cursor()

print("=== ACCOUNT INFO ===")
cur.execute("SELECT CURRENT_REGION(), CURRENT_ACCOUNT()")
row = cur.fetchone()
print(f"Region: {row[0]} | Account: {row[1]}")

print("\n=== CORTEX COMPLETE TEST ===")
try:
    cur.execute("SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3-8b', 'Say hello in 3 words') AS TEST")
    print("Cortex COMPLETE:", cur.fetchone()[0])
except Exception as e:
    print("Cortex COMPLETE error:", e)

print("\n=== CORTEX SEARCH SERVICES ===")
try:
    cur.execute("SHOW CORTEX SEARCH SERVICES IN DATABASE CHURN_DEMO")
    rows = cur.fetchall()
    for r in rows:
        print(" -", r[1])
except Exception as e:
    print("Error:", e)

print("\n=== STREAMLITS ===")
try:
    cur.execute("SHOW STREAMLITS IN DATABASE CHURN_DEMO")
    rows = cur.fetchall()
    for r in rows:
        print(" -", r[1], "| URL:", r)
except Exception as e:
    print("Error:", e)

print("\n=== CORTEX ANALYST (function check) ===")
try:
    cur.execute("SELECT SNOWFLAKE.CORTEX.ANALYST('test', '@CHURN_DEMO.PUBLIC.AGENT_ASSETS/semantic_model.yaml')")
    print("Analyst available:", cur.fetchone())
except Exception as e:
    print("Analyst status:", e)

conn.close()
