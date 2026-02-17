
import snowflake.connector

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

print("-" * 60)
print("🔍 VERIFYING INTELLIGENCE AGENT OUTPUT")
print("-" * 60)

try:
    cursor.execute("SELECT PREPARED_EMAIL FROM HIGH_RISK_ALERTS_HISTORY WHERE USER_ID = 'u_DEMO_VIDEO'")
    rows = cursor.fetchall()
    
    if rows and rows[0][0]:
        email = rows[0][0]
        print("✅ SUCCESS! The Agent generated this email:\n")
        print(email)
        print("\n" + "-" * 60)
        
        if "international transaction fees" in email.lower() or "fees" in email.lower():
            print("🌟 RAG CONFIRMED: The Agent used the context regarding 'fees'!")
        else:
            print("⚠ RAG WARNING: The Agent wrote a generic email (Context might have been missed).")
            
    else:
        print("❌ NO EMAIL FOUND. The Manual Trigger script likely crashed before saving.")
        
except Exception as e:
    print(f"Error: {e}")

conn.close()
