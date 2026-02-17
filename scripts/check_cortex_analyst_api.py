
import snowflake.connector
import sys
from pathlib import Path

def check_for_analyst_api():
    print("🔍 CHECKING FOR OFFICIAL CORTEX ANALYST API...")
    
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
        cursor.execute("SHOW FUNCTIONS IN SCHEMA SNOWFLAKE.CORTEX")
        funcs = cursor.fetchall()
        
        found = False
        print("\n[Available Cortex Functions]:")
        for f in funcs:
            if 'EMBED' in f[1] or 'COMPLETE' in f[1] or 'SEARCH' in f[1] or 'ANALYST' in f[1]:
                print(f" - {f[1]} ({f[8]})") # Name and Argument signature
            
            if 'ANALYST' in f[1].upper():
                found = True
                
        print("-" * 40)
        if found:
            print("✅ API FOUND! We can build the Official Cortex Analyst.")
        else:
            print("❌ API NOT FOUND. We must stick to the Custom Agent (Streamlit + Complete).")
            
    except Exception as e:
        print(f"❌ Error listing functions: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_for_analyst_api()
