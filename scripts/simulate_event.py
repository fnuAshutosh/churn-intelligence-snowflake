import snowflake.connector
import os
import random

# For demo, we might want to clean up env loading
# Assuming environment variables are set or using placeholders
# We can use the 'src.core.config' if we want to be fancy.
# But for a simple script, os.getenv is fine if we export vars first.

# We will just reuse the standard credentials pattern for simplicity
def get_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "BANKING"),
        database=os.getenv("SNOWFLAKE_DATABASE", "CHURN_DEMO"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
    )

def inject_demo_user(user_id="u_DEMO_VIDEO"):
    print(f"🚀 Injecting Demo User: {user_id}")
    conn = get_connection()
    cursor = conn.cursor()
    
    # 1. Insert High Risk Event
    # We cheat and insert directly into RAW table to simulate Kafka ingestion
    # Score 0.95 -> Critical Risk
    sql = f"""
    INSERT INTO CHURN_SCORES_RAW 
    (USER_ID, WINDOW_START, WINDOW_END, CHURN_SCORE, DECLINE_COUNT, DISPUTE_COUNT, SPEND_AMOUNT, INGESTION_TIME)
    VALUES 
    ('{user_id}', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 0.95, 12, 3, 50.0, CURRENT_TIMESTAMP())
    """
    try:
        cursor.execute(sql)
        print("✓ Data Ingested into CHURN_SCORES_RAW")
        print("  - Churn Score: 0.95 (CRITICAL)")
        print("  - Reason: High Declines (12)")
    except Exception as e:
        print(f"✗ Injection Failed: {e}")
    
    # 2. Force Refresh Dynamic Table (Make it faster than 1 min lag for demo)
    print("⏳ Triggering Dynamic Table Refresh...")
    try:
        cursor.execute("ALTER DYNAMIC TABLE CHURN_METRICS_LIVE REFRESH")
        print("✓ Refresh Triggered (Wait ~15s)")
    except Exception as e:
        print(f"⚠ Refresh Trigger warning: {e}")
        
    conn.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--user", default="u_DEMO_VIDEO", help="User ID to simulate")
    args = parser.parse_args()
    
    inject_demo_user(args.user)
