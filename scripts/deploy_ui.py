
import snowflake.connector
import os
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

from src.core.config import load_config

def deploy_frontend():
    print("🎨 DEPLOYING UI & SEMANTIC LAYERS...")
    
    config = load_config()
    conn = snowflake.connector.connect(**config['snowflake'])
    cursor = conn.cursor()

    try:
        # 1. Create Stage for Files
        print("-> Creating Stage @PROJECT_FILES...")
        cursor.execute("CREATE STAGE IF NOT EXISTS PROJECT_FILES DIRECTORY = (ENABLE = TRUE)")

        # 2. Upload Dashboard
        dashboard_path = PROJECT_ROOT / "src" / "app" / "dashboard.py"
        print(f"-> Uploading Dashboard: {dashboard_path}")
        # Windows formatting for PUT command
        dash_str = str(dashboard_path).replace("\\", "\\\\") 
        cursor.execute(f"PUT 'file://{dash_str}' @PROJECT_FILES AUTO_COMPRESS=FALSE OVERWRITE=TRUE")

        # 3. Upload Semantic Model
        yaml_path = PROJECT_ROOT / "semantic" / "churn_semantic_model.yaml"
        print(f"-> Uploading Semantic Model: {yaml_path}")
        yaml_str = str(yaml_path).replace("\\", "\\\\")
        cursor.execute(f"PUT 'file://{yaml_str}' @PROJECT_FILES AUTO_COMPRESS=FALSE OVERWRITE=TRUE")

        # 4. Create Streamlit App
        print("-> Creating Streamlit App Object...")
        streamlit_sql = """
        CREATE STREAMLIT IF NOT EXISTS CHURN_INTELLIGENCE_DASHBOARD
            ROOT_LOCATION = '@CHURN_DEMO.PUBLIC.PROJECT_FILES'
            MAIN_FILE = '/dashboard.py'
            QUERY_WAREHOUSE = 'BANK_WAREHOUSE'
        """
        cursor.execute(streamlit_sql)
        print("✅ Streamlit App Created! (Check 'Projects' in Snowflake UI)")

    except Exception as e:
        print(f"❌ DEPLOYMENT FAILED: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    deploy_frontend()
