
import snowflake.connector

def test_cross_region_llm():
    print("🌍 TESTING CROSS-REGION CORTEX ACCESS...")
    
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
    
    # Try a simple prompt
    prompt = "Explain why customer churn is bad in one sentence."
    
    try:
        print(f"-> Sending prompt to snowflake-arctic: '{prompt}'")
        # Note: Sometimes cross-region takes a minute to propagate
        cursor.execute(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('snowflake-arctic', '{prompt}')")
        response = cursor.fetchone()[0]
        
        print("\n" + "="*60)
        print("✅ SUCCESS! LLM RESPONSE:")
        print("="*60)
        print(response)
        print("="*60)
        print("🚀 CROSS-REGION INFERENCE IS LIVE!")
        
    except Exception as e:
        print(f"\n❌ FAILURE: {e}")
        print("Possible reasons: Propagation delay (wait 5 mins) or specific model unavailability.")

    conn.close()

if __name__ == "__main__":
    test_cross_region_llm()
