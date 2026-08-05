import os
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_PATH = PROJECT_ROOT / ".env"

load_dotenv(ENV_PATH)

DB_CONFIG = {
    "host": os.getenv("SOUERCE_DB_HOST", "localhost"),
    "port": int(os.getenv("SOUERCE_DB_PORT", "5435")),
    "database": os.getenv("SOURCE_DB_NAME","supermarket_source"),
    "user": os.getenv("SOURCE_DB_USER"),
    "password": os.getenv("SOURCE_DB_PASSWORD")
}