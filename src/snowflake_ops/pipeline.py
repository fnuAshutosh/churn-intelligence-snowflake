import snowflake.connector
from src.core.config import get_snowflake_connection_params

def get_connection():
    params = get_snowflake_connection_params()
    return snowflake.connector.connect(**params)

def setup_pipeline():
    conn = get_connection()
    cursor = conn.cursor()
    print("Setting up Snowflake Data Pipeline")

    try:
        # 1. Base Tables
        print("-> Configuring Base Tables...")
        cursor.execute("CREATE TABLE IF NOT EXISTS CHURN_SCORES_RAW (USER_ID VARCHAR, WINDOW_START VARCHAR, WINDOW_END VARCHAR, CHURN_SCORE FLOAT, DECLINE_COUNT INT, DISPUTE_COUNT INT, SPEND_AMOUNT FLOAT, INGESTION_TIME TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())")
        cursor.execute("ALTER TABLE CHURN_SCORES_RAW SET CHANGE_TRACKING = TRUE")

        # 2. History/Sink Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS HIGH_RISK_ALERTS_HISTORY (
                USER_ID VARCHAR,
                ALERT_TIME TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                RISK_SCORE FLOAT,
                REASON VARCHAR,
                PREPARED_EMAIL VARCHAR -- Added for GenAI Agent
            )
        """)

        # 3. Dynamic Table (ELT)
        print("-> Creating Dynamic Table (Declarative ETL)...")
        dt_sql = """
        CREATE OR REPLACE DYNAMIC TABLE CHURN_METRICS_LIVE
            TARGET_LAG = '1 minute'
            WAREHOUSE = BANKING
            AS
            SELECT 
                USER_ID,
                MAX(INGESTION_TIME) as LAST_UPDATED,
                AVG(CHURN_SCORE) as AVG_CHURN_SCORE_10M,
                MAX(CHURN_SCORE) as MAX_CHURN_SCORE_10M,
                SUM(DECLINE_COUNT) as TOTAL_DECLINES_10M,
                SUM(SPEND_AMOUNT) as TOTAL_SPEND_10M,
                CASE 
                    WHEN MAX(CHURN_SCORE) > 0.8 THEN 'CRITICAL'
                    WHEN MAX(CHURN_SCORE) > 0.5 THEN 'AT_RISK'
                    ELSE 'SAFE'
                END as RISK_STATUS
            FROM CHURN_SCORES_RAW
            WHERE INGESTION_TIME >= DATEADD(minute, -10, CURRENT_TIMESTAMP())
            GROUP BY USER_ID
        """
        cursor.execute(dt_sql)

        # 4. Streams (CDC)
        print("-> Configuring Streams...")
        cursor.execute("CREATE OR REPLACE STREAM CHURN_METRICS_STREAM ON DYNAMIC TABLE CHURN_METRICS_LIVE")

        # 5. Tasks (Automation)
        # Note: This task uses the GenAI function. We assume it exists or will handle error if not.
        print("-> Configuring Tasks...")
        task_sql = """
        CREATE OR REPLACE TASK PROCESS_ALERTS_TASK
            WAREHOUSE = BANKING
            SCHEDULE = '1 MINUTE'
        WHEN
            SYSTEM$STREAM_HAS_DATA('CHURN_METRICS_STREAM')
        AS
            INSERT INTO HIGH_RISK_ALERTS_HISTORY (USER_ID, RISK_SCORE, REASON, PREPARED_EMAIL)
            SELECT 
                USER_ID, 
                MAX_CHURN_SCORE_10M, 
                'CRITICAL_RISK_DETECTED',
                -- Safe call to GenAI Agent (if exists), else NULL
                CASE WHEN (SELECT COUNT(*) FROM INFORMATION_SCHEMA.FUNCTIONS WHERE FUNCTION_NAME = 'GENERATE_RETENTION_EMAIL') > 0 
                     THEN GENERATE_RETENTION_EMAIL(USER_ID, MAX_CHURN_SCORE_10M, 'CRITICAL_RISK_DETECTED')
                     ELSE NULL 
                END
            FROM CHURN_METRICS_STREAM
            WHERE METADATA$ACTION = 'INSERT' 
              AND RISK_STATUS = 'CRITICAL'
        """
        cursor.execute(task_sql)
        cursor.execute("ALTER TASK PROCESS_ALERTS_TASK RESUME")

        print("Pipeline Setup Complete")

    except Exception as e:
        print(f"Setup Failed: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    setup_pipeline()
