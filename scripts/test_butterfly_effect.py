
import snowflake.connector
import time
import sys
import random
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))
from src.core.config import load_config

def butterfly_test():
    print("🦋 BUTTERFLY EFFECT TEST: 11 Minute Simulation")
    print("==============================================")
    
    config = load_config()
    conn = snowflake.connector.connect(**config['snowflake'])
    cursor = conn.cursor()

    # 1. SETUP: Configure for High Frequency
    print("\n[Phase 0] Configuring for 1-minute responses...")
    try:
        cursor.execute("ALTER WAREHOUSE BANK_WAREHOUSE SET AUTO_SUSPEND = 60")
        cursor.execute("ALTER DYNAMIC TABLE CHURN_METRICS_LIVE SET TARGET_LAG = '1 minute'")
        cursor.execute("ALTER TASK PROCESS_ALERTS_TASK SET SCHEDULE = '1 MINUTE'")
        cursor.execute("ALTER TASK PROCESS_ALERTS_TASK RESUME")
        print("✅ System armed. Warehouse will auto-suspend after 60s of idle.")
    except Exception as e:
        print(f"Failed setup: {e}")
        return

    def inject(i):
        load = random.randint(1, 5)
        print(f"  -> key press... (Simulating {load} events)")
        cursor.execute(f"""
            INSERT INTO CHURN_SCORES_RAW (USER_ID, CHURN_SCORE, INGESTION_TIME)
            VALUES ('butterfly_{i}', {random.random()}, CURRENT_TIMESTAMP())
        """)

    # 2. PHASE 1: Active (5 Minutes)
    print("\n[Phase 1] Active Flapping (5 mins)...")
    for i in range(5):
        print(f"  Minute {i+1}/5: Injecting Data...")
        inject(i)
        time.sleep(60) 

    # 3. PHASE 2: Idle (5 Minutes)
    print("\n[Phase 2] Still Air / Idle (5 mins)...")
    print("  -> Stopping data injection.")
    print("  -> Observing Warehouse state...")
    for i in range(5):
        cursor.execute("SHOW WAREHOUSES LIKE 'BANK_WAREHOUSE'")
        state = cursor.fetchone()[4] # 'state' column
        print(f"  Minute {i+1}/5: Warehouse is {state}")
        time.sleep(60)

    # 4. PHASE 3: Re-Awakening
    print("\n[Phase 3] The Wake Up Call...")
    inject(99)
    print("  -> Data pushed. Waiting 60s for Task to fetch...")
    time.sleep(65)
    
    cursor.execute("SHOW WAREHOUSES LIKE 'BANK_WAREHOUSE'")
    state = cursor.fetchone()[4]
    print(f"  Result: Warehouse is {state} (Expected: STARTED/RESUMING)")

    # Cleanup
    print("\n[Cleanup] Returning to Safe Mode...")
    cursor.execute("ALTER DYNAMIC TABLE CHURN_METRICS_LIVE SET TARGET_LAG = 'DOWNSTREAM'")
    cursor.execute("ALTER TASK PROCESS_ALERTS_TASK SUSPEND")
    print("✅ Test Complete.")
    conn.close()

if __name__ == "__main__":
    butterfly_test()
