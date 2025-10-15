import os
from pathlib import Path
import sqlite3

BASE = Path(__file__).resolve().parents[1]
DB = BASE / "data" / "warehouse" / "adops.db"

def test_sqlite_exists():
    assert DB.exists(), "SQLite DB not found. Run: python -m etl.etl"

def test_table_has_rows():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM fact_metrics")
    n = cur.fetchone()[0]
    conn.close()
    assert n > 0, "No rows in fact_metrics"

def test_kpi_columns_present():
    import pandas as pd
    conn = sqlite3.connect(DB)
    df = pd.read_sql_query("SELECT * FROM fact_metrics LIMIT 1", conn)
    conn.close()
    for col in ["CTR","Fill_Rate","eCPM"]:
        assert col in df.columns
