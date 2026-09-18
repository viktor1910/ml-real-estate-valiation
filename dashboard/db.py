"""DB helpers for the Streamlit dashboards.

Pure builders (SQL string, month range) live at the top and are unit-tested;
connection-backed queries live below. Postgres connection values are reused
from ml/config.py — the single config source — so nothing here duplicates them.
"""
import os
import sys
from datetime import date

# ml/ on path so we reuse the single Postgres config source.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ml"))
from config import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD  # noqa: E402

UPSERT_MACRO_SQL = (
    "INSERT INTO macro_monthly (month, cpi, rate) VALUES (%s, %s, %s) "
    "ON CONFLICT (month) DO UPDATE SET cpi = EXCLUDED.cpi, rate = EXCLUDED.rate"
)

SEED_START = date(2025, 1, 1)


def month_options(today=None):
    """YYYY-MM strings from SEED_START through 12 months after `today`, ascending."""
    today = today or date.today()
    end = today.year * 12 + (today.month - 1) + 12
    start = SEED_START.year * 12 + (SEED_START.month - 1)
    return [f"{m // 12:04d}-{m % 12 + 1:02d}" for m in range(start, end + 1)]


# --- connection-backed helpers (Task 2) ---
import psycopg2  # noqa: E402
import pandas as pd  # noqa: E402


def get_connection():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD,
    )


def list_batches():
    q = ("SELECT DISTINCT model_version, scored_at FROM predictions "
         "ORDER BY scored_at DESC")
    with get_connection() as c:
        return pd.read_sql(q, c)


def load_predictions(model_version, scored_at):
    q = ("SELECT district, area_m2, listing_ppm2, predicted_ppm2, "
         "undervalued_ratio, is_undervalued, url, model_version, scored_at "
         "FROM predictions WHERE model_version = %s AND scored_at = %s")
    with get_connection() as c:
        return pd.read_sql(q, c, params=(model_version, scored_at))


def load_macro():
    with get_connection() as c:
        return pd.read_sql(
            "SELECT month, cpi, rate FROM macro_monthly ORDER BY month", c)


def upsert_macro(month, cpi, rate):
    with get_connection() as c:
        with c.cursor() as cur:
            cur.execute(UPSERT_MACRO_SQL, (month, float(cpi), float(rate)))
        c.commit()
