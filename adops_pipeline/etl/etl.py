import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
from adops_pipeline.etl.config import RAW_FILES, WH_DIR, SQLITE_PATH, PARQUET_PATH, TABLE_NAME

def extract(files):
    frames = []
    for f in files:
        df = pd.read_csv(f)
        df["__source_file"] = f.name
        frames.append(df)
    return pd.concat(frames, ignore_index=True)

def transform(df: pd.DataFrame):
    num_cols = ["requests", "impressions", "clicks", "revenue"]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["source"] = df["source"].str.lower().str.strip()
    df["country"] = df["country"].str.upper().str.strip()

    df["CTR"] = (df["clicks"] / df["impressions"]).fillna(0).replace([pd.NA, pd.NaT], 0)
    df["Fill_Rate"] = (df["impressions"] / df["requests"]).fillna(0)
    df["eCPM"] = (df["revenue"] / df["impressions"] * 1000).fillna(0)

    df = df.drop_duplicates(subset=["campaign_id", "source", "country", "date"])

    cols = [
        "campaign_id","source","country","date",
        "requests","impressions","clicks","revenue",
        "CTR","Fill_Rate","eCPM","__source_file"
    ]
    return df[cols]

def load(df: pd.DataFrame):
    WH_DIR.mkdir(parents=True, exist_ok=True)

    # כתיבה ל-SQLite
    engine = create_engine(f"sqlite:///{SQLITE_PATH}")
    with engine.begin() as conn:
        df.to_sql(TABLE_NAME, conn, if_exists="replace", index=False)

    # כתיבה ל-Parquet (לניתוחים מהירים)
    try:
        df.to_parquet(PARQUET_PATH, index=False)  # דורש pyarrow
    except Exception as e:
        print("Parquet write failed (install pyarrow?):", e)

def run():
    df_raw = extract(RAW_FILES)
    df_tr  = transform(df_raw)
    load(df_tr)
    print(f"Loaded {len(df_tr):,} rows → {TABLE_NAME}")
    print(f"SQLite: {SQLITE_PATH}")
    print(f"Parquet: {PARQUET_PATH}")

if __name__ == "__main__":
    run()
