import os
import toml
from pathlib import Path
from typing import Dict, Any

# Root Config Path
CONFIG_DIR = Path(__file__).resolve().parent.parent.parent / "configs"
CONFIG_FILE = CONFIG_DIR / "settings.toml"
ENV_FILE = CONFIG_DIR / ".env"

def load_env_file():
    """Manual .env loader to avoid python-dotenv dependency if not present."""
    if ENV_FILE.exists():
        with open(ENV_FILE, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, value = line.split("=", 1)
                    os.environ[key.strip()] = value.strip()

def load_config() -> Dict[str, Any]:
    """
    Loads configuration from settings.toml and merges environment variables.
    """
    load_env_file() # Load .env before reading toml if it exists
    
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(f"Configuration file not found at: {CONFIG_FILE}")

    with open(CONFIG_FILE, "r") as f:
        config = toml.load(f)

    # Snowflake Credentials (Env Override Priority)
    config['snowflake']['account'] = os.getenv('SNOWFLAKE_ACCOUNT', config['snowflake'].get('account'))
    config['snowflake']['user'] = os.getenv('SNOWFLAKE_USER', config['snowflake'].get('user'))
    config['snowflake']['password'] = os.getenv('SNOWFLAKE_PASSWORD')
    config['snowflake']['role'] = os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')
    config['snowflake']['warehouse'] = os.getenv('SNOWFLAKE_WAREHOUSE', 'BANKING')
    config['snowflake']['database'] = os.getenv('SNOWFLAKE_DATABASE', 'CHURN_DEMO')
    config['snowflake']['schema'] = os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')

    # Kafka Credentials (Env Override Priority)
    config['kafka']['bootstrap_servers'] = os.getenv('KAFKA_BOOTSTRAP_SERVERS', config['kafka']['bootstrap_servers'])
    config['kafka']['sasl_username'] = os.getenv('KAFKA_USER')
    config['kafka']['sasl_password'] = os.getenv('KAFKA_PASS')

    return config

def get_snowflake_connection_params():
    config = load_config()
    return config['snowflake']
