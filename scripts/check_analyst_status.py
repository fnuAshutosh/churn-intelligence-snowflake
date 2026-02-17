
import snowflake.connector
import sys
from pathlib import Path

def check_analyst_status():
    print("🔍 CHECKING CORTEX ANALYST STATUS...")
    
    conn = snowflake.connector.connect(
        user='ASHUTOSHEVE',
        password='uLJxnuda-28DBKW',
        account='BGBSVSB-VZ51957',
        role='ACCOUNTADMIN',
        warehouse='BANK_WAREHOUSE',
        database='CHURN_DEMO',
        schema='PUBLIC'
    )
    cursor = conn.cursor()
    
    try:
        cursor.execute("SHOW FUNCTIONS LIKE 'ANALYST' IN SCHEMA SNOWFLAKE.CORTEX")
        rows = cursor.fetchall()
        
        if rows:
            print(f"✅ OFFICIAL API FOUND: {rows[0][1]}")
            print("   You CAN use the YAML file directly.")
        else:
            print("❌ OFFICIAL API NOT FOUND (Restricted/Unavailable in Region).")
            print("   You MUST build the Custom Agent (Manual Implementation).")
            print("   (This is what we have already built in dashboard.py)")

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_analyst_status()
