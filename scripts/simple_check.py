import sys
from pathlib import Path
import snowflake.connector

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

from src.core.config import load_config

config = load_config()
conf = config['snowflake']

conn = snowflake.connector.connect(
    account=conf['account'],
    user=conf['user'],
    password=conf['password'],
    role=conf['role'],
    warehouse=conf['warehouse'],
    database=conf['database'],
    schema=conf['schema']
)

cursor = conn.cursor()
print("Checking for u_DEMO_VIDEO email...")
cursor.execute("SELECT USER_ID, PREPARED_EMAIL FROM HIGH_RISK_ALERTS_HISTORY WHERE USER_ID = 'u_DEMO_VIDEO'")
rows = cursor.fetchall()

if rows:
    email_content = rows[0][1]
    print(f"✅ FOUND! Full Email Content:\n{'-'*60}\n{email_content}\n{'-'*60}")
else:
    print("⏳ Not found yet. Task might still be running.")

conn.close()
