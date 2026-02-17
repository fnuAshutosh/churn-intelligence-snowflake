
import sys
from pathlib import Path
import snowflake.connector

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))
print(f"DEBUG: Added {PROJECT_ROOT} to sys.path")

from src.core.config import load_config

def force_agent_response():
    config = load_config()
    conn = snowflake.connector.connect(**config['snowflake'])
    cursor = conn.cursor()
    
    user_id = 'u_DEMO_VIDEO'
    
    print(f"\n⚡ FORCING AGENT RESPONSE FOR: {user_id}")
    
    # 1. Verify Context Exists
    cursor.execute(f"SELECT TRANSCRIPT_TEXT FROM CUSTOMER_INTERACTION_TRANSCRIPTS WHERE USER_ID = '{user_id}'")
    ctx = cursor.fetchone()
    if ctx:
        print(f"📖 Context Found: {ctx[0][:60]}...")
    else:
        print("⚠ No Context Found! (RAG will be generic)")
        
    # 2. Call the Snowflake Function Directly
    print("🤖 Invoking Llama 3 via Cortex...")
    sql = f"""
    SELECT GENERATE_RETENTION_EMAIL(
        '{user_id}', 
        0.95, 
        'High Declines',
        (SELECT LISTAGG(TRANSCRIPT_TEXT, '. ') FROM CUSTOMER_INTERACTION_TRANSCRIPTS WHERE USER_ID = '{user_id}')
    )
    """
    cursor.execute(sql)
    email = cursor.fetchone()[0]
    
    print("\n" + "="*60)
    print(email)
    print("="*60)
    
    # 3. Save it so the dashboard sees it
    cursor.execute(f"""
        UPDATE HIGH_RISK_ALERTS_HISTORY 
        SET PREPARED_EMAIL = %s 
        WHERE USER_ID = %s
    """, (email, user_id))
    
    # If update didn't hit (no row exists), insert it
    if cursor.rowcount == 0:
         cursor.execute(f"""
            INSERT INTO HIGH_RISK_ALERTS_HISTORY (USER_ID, RISK_SCORE, REASON, PREPARED_EMAIL, ALERT_TIME)
            VALUES (%s, 0.95, 'High Declines', %s, CURRENT_TIMESTAMP())
        """, (user_id, email))
         
    print("✅ Saved to Dashboard History.")
    conn.close()

if __name__ == "__main__":
    force_agent_response()
