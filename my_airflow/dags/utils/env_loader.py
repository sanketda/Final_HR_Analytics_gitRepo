import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_env_file():
    """Manually load .env file if it exists in expected locations."""
    possible_paths = [
        os.path.join(os.getcwd(), ".env"),
        os.path.join(os.path.dirname(__file__), ".env"),
        os.path.join(os.path.dirname(__file__), "../../../.env"), 
        os.path.join(os.path.dirname(__file__), "../../.env"),
        "/opt/airflow/.env",
        "/opt/airflow/dags/utils/.env"
    ]
    for path in possible_paths:
        if os.path.exists(path):
            logger.info(f"Loading environment from {path}")
            with open(path) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, value = line.split("=", 1)
                        os.environ[key] = value.strip()
            return True
    logger.warning("No .env file found in expected locations.")
    return False

# Auto-load on import
load_env_file()
