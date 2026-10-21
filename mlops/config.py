import os
from pathlib import Path
MODEL_DIR = Path(os.getenv("MODEL_DIR", "/models"))
REPORT_DIR = Path(os.getenv("REPORT_DIR", "/reports"))
CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_USER = os.getenv("CH_USER", "analyst")
CH_PASSWORD = os.getenv("CH_PASSWORD", "analyst_password")
