from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
WH_DIR  = BASE_DIR / "data" / "warehouse"

SQLITE_PATH = WH_DIR / "adops.db"
PARQUET_PATH = WH_DIR / "metrics.parquet"
TABLE_NAME = "fact_metrics"

RAW_FILES = [
    RAW_DIR / "campaigns_il.csv",
    RAW_DIR / "campaigns_us.csv",
    RAW_DIR / "campaigns_uk.csv",
]
