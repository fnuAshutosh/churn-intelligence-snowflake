import snowflake.connector
from src.core.config import get_snowflake_connection_params

def setup_agent():
    params = get_snowflake_connection_params()
    conn = snowflake.connector.connect(**params)
    cursor = conn.cursor()
    print("Deploying Cortex Agent...")
    
    cursor.execute(f"USE DATABASE {params.get('database', 'CHURN_DEMO')}")
    cursor.execute(f"USE SCHEMA {params.get('schema', 'PUBLIC')}")

    # Define Llama 3 Prompt with RAG Context
    prompt_template = """
    'You are a retention specialist for a premium bank.
    
    User ID: ' || user_id || '
    Risk Score: ' || risk_score || '
    Reason: ' || reason || '
    
    Latest Interaction Context: ' || COALESCE(customer_context, 'No recent support tickets found.') || '
    
    Task: Write a short, empathetic email offering a solution. 
    IF the context mentions specific complaints (e.g., fees), address them directly and offer a relevant solution (e.g., fee waiver).
    OTHERWISE, offer a generic $50 statement credit.
    
    Sign off as "The Data Team".'
    """
    
    sql_func = f"""
    CREATE OR REPLACE FUNCTION GENERATE_RETENTION_EMAIL(user_id VARCHAR, risk_score FLOAT, reason VARCHAR, customer_context VARCHAR)
    RETURNS VARCHAR
    LANGUAGE SQL
    AS
    $$
        SNOWFLAKE.CORTEX.COMPLETE('snowflake-arctic', {prompt_template})
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
