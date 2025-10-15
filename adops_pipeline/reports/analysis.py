from pathlib import Path
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

# --- Paths ---
BASE = Path(__file__).resolve().parents[1]
PARQUET = BASE / "data" / "warehouse" / "metrics.parquet"
SQLITE  = BASE / "data" / "warehouse" / "adops.db"
CHARTS  = BASE / "reports" / "charts"

# Matplotlib defaults
plt.rcParams.update({
    "figure.figsize": (10, 6),
    "axes.titleweight": "bold",
    "axes.grid": True,
    "grid.alpha": 0.25,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.labelsize": 11,
    "xtick.labelsize": 10,
    "ytick.labelsize": 10,
})

def load_df() -> pd.DataFrame:
    """Load dataset from Parquet if available, otherwise from SQLite."""
    if PARQUET.exists():
        return pd.read_parquet(PARQUET)
    if SQLITE.exists():
        with sqlite3.connect(SQLITE) as conn:
            return pd.read_sql_query("SELECT * FROM fact_metrics", conn)
    raise FileNotFoundError("No data source found. Run the ETL first.")

def _format_x_labels(ax, rotation: int = 0):
    """Keep x labels horizontal and readable with extra bottom margin."""
    for lbl in ax.get_xticklabels():
        lbl.set_rotation(rotation)
        lbl.set_ha("center")
    plt.tight_layout()
    plt.subplots_adjust(bottom=0.15)

def _add_value_labels(ax, fmt: str = "{:.2f}", pad: float = 0.5):
    """Write value labels above bars (y values)."""
    for rect in ax.patches:
        height = rect.get_height()
        if pd.isna(height):
            continue
        ax.annotate(
            fmt.format(height),
            (rect.get_x() + rect.get_width() / 2, height),
            ha="center", va="bottom",
            xytext=(0, pad),
            textcoords="offset points",
            fontsize=9,
        )

def chart_ctr_by_campaign(df: pd.DataFrame, top_n: int = 12):
    ser = (
        df.groupby("campaign_id")["CTR"]
          .mean()
          .sort_values(ascending=False)
          .head(top_n)
    )
    fig, ax = plt.subplots(figsize=(14, 6))  # ← רוחב גדול יותר
    bars = ax.bar(ser.index, ser.values, width=0.5)
    ax.set_title("CTR by Campaign", fontsize=14, weight="bold")
    ax.set_xlabel("Campaign", fontsize=11)
    ax.set_ylabel("CTR", fontsize=11)
    ax.set_ylim(0, ser.max() * 1.2)  # מעט מקום מעל העמודות
    ax.grid(True, axis="y", linestyle="--", alpha=0.25)

    # שמירת מרווח נכון לתוויות X
    plt.xticks(rotation=0, ha="center")
    plt.subplots_adjust(bottom=0.25)  # מרווח גדול יותר בתחתית

    # תוויות ערכים מעל כל עמודה
    for rect in bars:
        height = rect.get_height()
        ax.annotate(
            f"{height:.3f}",
            xy=(rect.get_x() + rect.get_width() / 2, height),
            xytext=(0, 5),
            textcoords="offset points",
            ha="center",
            va="bottom",
            fontsize=9,
            color="#e5e7eb"
        )

    CHARTS.mkdir(parents=True, exist_ok=True)
    out = CHARTS / "ctr_by_campaign.png"
    plt.tight_layout()
    plt.savefig(out, dpi=180, bbox_inches="tight")
    plt.close()
    print(f"Saved → {out}")


def chart_ecpm_by_country(df: pd.DataFrame):
    ser = (
        df.groupby("country")["eCPM"]
          .mean()
          .sort_values(ascending=False)
    )
    ax = ser.plot(kind="bar")
    ax.set_title("eCPM by Country")
    ax.set_xlabel("Country")
    ax.set_ylabel("eCPM")
    _format_x_labels(ax, rotation=0)
    _add_value_labels(ax, fmt="{:.2f}")
    CHARTS.mkdir(parents=True, exist_ok=True)
    out = CHARTS / "ecpm_by_country.png"
    plt.tight_layout()
    plt.savefig(out, dpi=160)
    plt.close()
    print(f"Saved → {out}")

def chart_fillrate_by_source(df: pd.DataFrame):
    ser = (
        df.groupby("source")["Fill_Rate"]
          .mean()
          .sort_values(ascending=False)
    )
    ax = ser.plot(kind="bar")
    ax.set_title("Fill Rate by Source")
    ax.set_xlabel("Source")
    ax.set_ylabel("Fill Rate")
    _format_x_labels(ax, rotation=0)
    _add_value_labels(ax, fmt="{:.3f}")
    CHARTS.mkdir(parents=True, exist_ok=True)
    out = CHARTS / "fillrate_by_source.png"
    plt.tight_layout()
    plt.savefig(out, dpi=160)
    plt.close()
    print(f"Saved → {out}")

def main():
    df = load_df()
    print("Sample rows:")
    print(df.head(6))

    chart_ctr_by_campaign(df, top_n=12)
    chart_ecpm_by_country(df)
    chart_fillrate_by_source(df)

    print(f"Charts written to: {CHARTS}")

if __name__ == "__main__":
    main()
