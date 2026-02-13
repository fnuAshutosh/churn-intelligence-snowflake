import sys
import os
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

# Now we can import from src
try:
    from src.snowflake_ops.pipeline import setup_pipeline
    from src.ai.agent import setup_agent
    from src.ai.ml_model import setup_ml_pipeline
except ImportError as e:
    print(f"Import Error: {e}. Check PYTHONPATH.")
    sys.exit(1)

def main():
    print("=" * 60)
    print("Snowflake Native Churn Intelligence - Deployment Orchestrator")
    print("=" * 60)
    
    # 1. Pipeline (Tables -> DTs -> Streams -> Tasks)
    print("\n[Stage 1/3] Deploying Data Engineering Pipeline...")
    setup_pipeline()
    
    # 2. AI Agent (Cortex)
    print("\n[Stage 2/3] Deploying Cortex AI Agent...")
    setup_agent()
    
    # 3. Machine Learning (Snowpark)
    print("\n[Stage 3/3] Deploying ML Models & Inference...")
    setup_ml_pipeline()
    
    print("\n" + "=" * 60)
    print("✅ DEPLOYMENT SUCCESSFUL")
    print("=" * 60)
    print("Next Steps:")
    print("1. Start Streamlit App: `streamlit run src/app/dashboard.py`")
    print("2. Run Ingestion (Optional): `python src/ingestion/consumer.py`")

if __name__ == "__main__":
    main()
