import snowflake.connector
from src.core.config import get_snowflake_connection_params

def setup_agent():
    params = get_snowflake_connection_params()
    conn = snowflake.connector.connect(**params)
    cursor = conn.cursor()
    print("Deploying Cortex Agent...")

    # Define Llama 3 Prompt
    prompt_template = """
    'You are a retention specialist for a premium bank.
    User ID: ' || user_id || '.
    Risk Score: ' || risk_score || '.
    Reason: ' || reason || '.
    Write a short, polite email offering a $50 statement credit.
    Sign off as "The Data Team".'
    """
    
    sql_func = f"""
    CREATE OR REPLACE FUNCTION GENERATE_RETENTION_EMAIL(user_id VARCHAR, risk_score FLOAT, reason VARCHAR)
    RETURNS VARCHAR
    LANGUAGE SQL
    AS
    $$
        SNOWFLAKE.CORTEX.COMPLETE('llama3-8b', {prompt_template})
    $$;
    """

    try:
        cursor.execute(sql_func)
        print("Created Function: GENERATE_RETENTION_EMAIL (Llama 3-8b)")
    except Exception as e:
        print(f"Failed to create Agent: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    setup_agent()
