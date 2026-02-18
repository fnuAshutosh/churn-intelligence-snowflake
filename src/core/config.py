"""
Central configuration — reads Snowflake credentials from environment variables.
Works both locally (via .env file) and inside Docker (via env_file / environment).
"""
import os
from dotenv import load_dotenv

# Load .env if it exists (local dev). In Docker, env vars are injected directly.
load_dotenv()


def get_snowflake_connection_params() -> dict:
    """Return Snowflake connector kwargs from environment variables."""
    required = ["SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_ACCOUNT"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise EnvironmentError(f"Missing required env vars: {missing}")

    return {
        "user":      os.environ["SNOWFLAKE_USER"],
        "password":  os.environ["SNOWFLAKE_PASSWORD"],
        "account":   os.environ["SNOWFLAKE_ACCOUNT"],
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "BANK_WAREHOUSE"),
        "database":  os.getenv("SNOWFLAKE_DATABASE",  "CHURN_DEMO"),
        "schema":    os.getenv("SNOWFLAKE_SCHEMA",     "PUBLIC"),
    }
