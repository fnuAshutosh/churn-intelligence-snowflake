import snowflake.connector
import os

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    role=os.getenv("SNOWFLAKE_ROLE"),
    warehouse='BANKING',
    database='CHURN_DEMO',
    schema='PUBLIC'
)

cursor = conn.cursor()
print("Checking for u_DEMO_VIDEO email...")
cursor.execute("SELECT USER_ID, PREPARED_EMAIL FROM HIGH_RISK_ALERTS_HISTORY WHERE USER_ID = 'u_DEMO_VIDEO'")
rows = cursor.fetchall()

if rows:
    print(f"✅ FOUND! Email: {rows[0][1][:50]}...")
else:
    print("⏳ Not found yet. Task might still be running.")

conn.close()
