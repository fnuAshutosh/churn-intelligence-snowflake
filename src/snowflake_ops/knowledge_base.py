
import snowflake.connector
from src.core.config import get_snowflake_connection_params

def setup_knowledge_base():
    params = get_snowflake_connection_params()
    # Connect without DB first to ensure creation
    base_params = params.copy()
    db_name = base_params.pop('database', 'CHURN_DEMO')
    schema_name = base_params.pop('schema', 'PUBLIC')
    
    conn = snowflake.connector.connect(**base_params)
    cursor = conn.cursor()
    
    print(f"Deploying Cortex Search Engine on {db_name}.{schema_name}")

    try:
        cursor.execute(f"USE DATABASE {db_name}")
        cursor.execute(f"USE SCHEMA {schema_name}")

        # 1. Unstructured Data Store (Transcripts/Tickets)
        print("-> Creating CUSTOMER_INTERACTION_TRANSCRIPTS...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS CUSTOMER_INTERACTION_TRANSCRIPTS (
                USER_ID VARCHAR,
                TRANSCRIPT_TEXT VARCHAR,
                INTERACTION_TYPE VARCHAR, -- Support, Email, Chat
                CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)

        # 2. Cortex Search Service (The Brain for Unstructured Data)
        # Note: This requires Enterprise+ and Cortex enabled
        print("-> Configuring Cortex Search Service...")
        search_service_sql = """
        CREATE OR REPLACE CORTEX SEARCH SERVICE CHURN_KNOWLEDGE_BASE
            ON TRANSCRIPT_TEXT
            ATTRIBUTES INTERACTION_TYPE
            WAREHOUSE = BANK_WAREHOUSE
            TARGET_LAG = '1 day' -- Search index refreshes daily to save costs
            AS (
                SELECT USER_ID, TRANSCRIPT_TEXT, INTERACTION_TYPE
                FROM CUSTOMER_INTERACTION_TRANSCRIPTS
            )
        """
        try:
            cursor.execute(search_service_sql)
            print("✅ Created Cortex Search Service: CHURN_KNOWLEDGE_BASE")
        except Exception as e:
            print(f"⚠ Cortex Search Service warning: {e} (Ensure account is Enterprise+)")

        # 3. Insert some demo knowledge context
        print("-> Backfilling Knowledge Base with demo context...")
        cursor.execute("""
            INSERT INTO CUSTOMER_INTERACTION_TRANSCRIPTS (USER_ID, TRANSCRIPT_TEXT, INTERACTION_TYPE)
            SELECT 'u_DEMO_VIDEO', 'Customer complained about high international transaction fees. Mentioned looking at competitor banks.', 'Support'
            WHERE NOT EXISTS (SELECT 1 FROM CUSTOMER_INTERACTION_TRANSCRIPTS WHERE USER_ID = 'u_DEMO_VIDEO')
        """)

    except Exception as e:
        print(f"Setup Failed: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    setup_knowledge_base()
