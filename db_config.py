# config.py
"""
Central configuration file for database connections and API endpoints.
Copy this file to config.local.py and edit values for your environment.
"""

import os
from dotenv import load_dotenv
from urllib.parse import quote

# Load environment variables from .env if present
load_dotenv()

# API endpoints
STOCK_SVC_URL = os.getenv("STOCK_SVC_URL", "https://api.example.com/stock")

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# URL-encode the password
ENCODED_PASSWORD = quote(DB_PASSWORD)

# Database connection strings
DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{ENCODED_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

DB_CONNECTION_STRING1 = (
    "postgresql+psycopg2://sd_admin:%s@192.168.1.72:5430/stock-dumps"
    % quote("sdadmin@postgres")
)
DB_CONNECTION_STRING = DATABASE_URL