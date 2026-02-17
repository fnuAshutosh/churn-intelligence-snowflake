
import snowflake.connector

def run_agent_hard():
    print("🤖 STARTING AGENT (HARDCODED MODE)...")
    
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
    
    user_id = 'u_DEMO_VIDEO'
    
    try:
        # 1. Get Context
        print("-> Fetching Context...")
        cursor.execute(f"SELECT TRANSCRIPT_TEXT FROM CUSTOMER_INTERACTION_TRANSCRIPTS WHERE USER_ID = '{user_id}'")
        row = cursor.fetchone()
        context_text = row[0] if row else "NO CONTEXT"
        print(f"   Context: {context_text[:50]}...")
        
        # 2. Run Cortex
        print("-> Running Llama 3 on Cortex...")
        rag_sql = f"""
        SELECT GENERATE_RETENTION_EMAIL(
            '{user_id}', 
            0.95, 
            'CRITICAL_RISK_DETECTED', 
            '{context_text.replace("'", "''")}'
        )
        """
        cursor.execute(rag_sql)
        email = cursor.fetchone()[0]
        
        print("\n" + "="*60)
        print("📧 GENERATED EMAIL:")
        print(email)
        print("="*60)
        
        # 3. Save
        print("-> Saving...")
        cursor.execute("DELETE FROM HIGH_RISK_ALERTS_HISTORY WHERE USER_ID = 'u_DEMO_VIDEO'")
        cursor.execute("INSERT INTO HIGH_RISK_ALERTS_HISTORY (USER_ID, RISK_SCORE, REASON, PREPARED_EMAIL, ALERT_TIME) VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP())", (user_id, 0.95, 'CRITICAL_RISK_DETECTED', email))
        print("✅ Saved successfully!")
        
    except Exception as e:
        print(f"❌ FAILED: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    run_agent_hard()
