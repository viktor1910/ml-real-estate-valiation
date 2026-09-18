import os
import sys
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dashboard"))

import db


def test_upsert_sql_is_parameterized_upsert():
    sql = db.UPSERT_MACRO_SQL.lower()
    assert "insert into macro_monthly" in sql
    assert "on conflict (month) do update" in sql
    # bound params, not interpolation
    assert "%s" in sql
    assert "{" not in db.UPSERT_MACRO_SQL and "format" not in db.UPSERT_MACRO_SQL


def test_month_options_range_boundaries():
    opts = db.month_options(today=date(2026, 1, 15))
    assert opts[0] == "2025-01"          # seed start
    assert opts[-1] == "2027-01"         # today + 12 months
    assert "2026-01" in opts
    assert opts == sorted(opts)          # ascending
    assert len(opts) == len(set(opts))   # no dupes
