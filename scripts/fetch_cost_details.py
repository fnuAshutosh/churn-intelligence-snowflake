
import snowflake.connector
import os

def get_billing_data():
    conn_params = {
        "account": "rwzcwwx-tv97893",
        "user": "AN05893N",
        "password": "Heart231995@beats",
        "role": "ACCOUNTADMIN",
        "warehouse": "BANKING"
    }

    try:
        conn = snowflake.connector.connect(**conn_params)
        cursor = conn.cursor()
        
        print("\n" + "="*80)
        print("SNOWFLAKE COST & BILLING REPORT (Last 30 Days)")
        print("="*80)

        # 1. Total Credits by Service Type
        print("\n[1] TOTAL CREDITS BY SERVICE TYPE")
        query1 = """
        SELECT SERVICE_TYPE, SUM(CREDITS_USED) as TOTAL_CREDITS
        FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
        WHERE USAGE_DATE >= DATEADD(day, -30, CURRENT_DATE())
        GROUP BY 1 ORDER BY 2 DESC;
        """
        cursor.execute(query1)
        print(f"{'Service Type':<30} | {'Total Credits':<15}")
        print("-" * 50)
        for row in cursor.fetchall():
            print(f"{str(row[0]):<30} | {float(row[1]):<15.4f}")

        # 2. Credits by Warehouse
        print("\n[2] CREDITS BY WAREHOUSE")
        query2 = """
        SELECT WAREHOUSE_NAME, SUM(CREDITS_USED) as CREDITS
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
        WHERE START_TIME >= DATEADD(day, -30, CURRENT_DATE())
        GROUP BY 1 ORDER BY 2 DESC;
        """
        cursor.execute(query2)
        print(f"{'Warehouse Name':<30} | {'Credits':<15}")
        print("-" * 50)
        for row in cursor.fetchall():
            print(f"{str(row[0]):<30} | {float(row[1]):<15.4f}")

        # 3. Dynamic Table Refresh Costs
        print("\n[3] DYNAMIC TABLE REFRESH COSTS")
        query3 = """
        SELECT NAME as TABLE_NAME, COUNT(*) as REFRESH_COUNT, SUM(CREDITS_USED) as TOTAL_CREDITS
        FROM SNOWFLAKE.ACCOUNT_USAGE.DYNAMIC_TABLE_REFRESH_HISTORY
        WHERE REFRESH_START_TIME >= DATEADD(day, -30, CURRENT_DATE())
        GROUP BY 1 ORDER BY 3 DESC;
        """
        try:
            cursor.execute(query3)
            print(f"{'Table Name':<30} | {'Refreshes':<10} | {'Credits':<15}")
            print("-" * 60)
            for row in cursor.fetchall():
                print(f"{str(row[0]):<30} | {int(row[1]):<10} | {float(row[2]):<15.4f}")
        except Exception as e:
            print(f"No DT usage found or error: {e}")

        # 4. Cortex AI Usage
        print("\n[4] CORTEX AI USAGE")
        query4 = """
        SELECT MODEL_NAME, SUM(CREDITS_USED_COMPUTE) as COMPUTE_CREDITS
        FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_USAGE_HISTORY
        WHERE START_TIME >= DATEADD(day, -30, CURRENT_DATE())
        GROUP BY 1 ORDER BY 2 DESC;
        """
        try:
            cursor.execute(query4)
            print(f"{'Model Name':<30} | {'Credits':<15}")
            print("-" * 50)
            for row in cursor.fetchall():
                print(f"{str(row[0]):<30} | {float(row[1]):<15.4f}")
        except Exception as e:
            print(f"No Cortex usage found or error: {e}")

        # 5. Daily Trend
        print("\n[5] DAILY SPENDING TREND (Last 14 Days)")
        query5 = """
        SELECT USAGE_DATE, SUM(CREDITS_USED) as DAILY_CREDITS
        FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
        WHERE USAGE_DATE >= DATEADD(day, -14, CURRENT_DATE())
        GROUP BY 1 ORDER BY 1 DESC;
        """
        cursor.execute(query5)
        print(f"{'Date':<15} | {'Daily Credits':<15}")
        print("-" * 35)
        for row in cursor.fetchall():
            print(f"{str(row[0]):<15} | {float(row[1]):<15.4f}")

    except Exception as e:
        print(f"FAILED: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    get_billing_data()
