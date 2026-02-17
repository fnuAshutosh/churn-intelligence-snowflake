
import snowflake.connector
import sys
import os
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))
print(f"DEBUG: Added {PROJECT_ROOT} to sys.path")

from src.core.config import load_config

def manual_agent_run():
    config = load_config()
    conf = config['snowflake']
    conn = snowflake.connector.connect(**conf)
    cursor = conn.cursor()
    
    print("\n🔍 Manually Running Agent Logic (Bypassing Task Scheduler)...")

    # 1. Inspect Stream Content
    print("-> Checking Stream Content...")
    try:
        cursor.execute("SELECT * FROM CHURN_METRICS_STREAM WHERE RISK_STATUS = 'CRITICAL'")
        rows = cursor.fetchall()
        
        if rows:
            print(f"✅ Found {len(rows)} rows in Stream.")
            user_id = rows[0][0]
        else:
            print("⚠ Stream is empty! Checking LIVE table directly...")
            cursor.execute("SELECT * FROM CHURN_METRICS_LIVE WHERE RISK_STATUS = 'CRITICAL'")
            rows = cursor.fetchall()
            if rows:
                print(f"✅ Found {len(rows)} high-risk users in LIVE table.")
                user_id = rows[0][0] # Assuming USER_ID is first col
            else:
                print("❌ No high-risk users found anywhere. Ensure Injection Script ran successfully.")
                conn.close()
                return

        # 2. Run the RAG Generation
        # We call the function directly
        print(f"-> Generating Email for {user_id}...")
        
        # Get Context first to show debug
        cursor.execute(f"SELECT TRANSCRIPT_TEXT FROM CUSTOMER_INTERACTION_TRANSCRIPTS WHERE USER_ID = '{user_id}'")
        ctx = cursor.fetchone()
        context_text = ctx[0] if ctx else "NO CONTEXT FOUND"
        print(f"   [Context Found]: {context_text}")

        # Generate Email
        rag_sql = f"""
        SELECT GENERATE_RETENTION_EMAIL(
            '{user_id}', 
            0.95, 
            'CRITICAL_RISK_DETECTED', 
            '{context_text.replace("'", "''")}'
        )
        """
        cursor.execute(rag_sql)
        email_result = cursor.fetchone()
        
        if email_result:
            email = email_result[0]
            print("\n" + "="*60)
            print("📧 GENERATED EMAIL (proof of Intelligence):")
            print("="*60)
            print(email)
            print("="*60)
            
            # 3. Save to History
            print("-> Saving to History Table...")
            # Use query parameter binding to handle quotes safely
            cursor.execute(
                "INSERT INTO HIGH_RISK_ALERTS_HISTORY (USER_ID, RISK_SCORE, REASON, PREPARED_EMAIL, ALERT_TIME) VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP())",
                (user_id, 0.95, 'CRITICAL_RISK_DETECTED', email)
            )
            print("✅ Saved!")
        else:
            print("❌ Agent returned NULL email.")
            
    except Exception as e:
        print(f"❌ ERROR: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    manual_agent_run()
