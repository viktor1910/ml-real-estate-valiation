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


def test_upsert_user_sql_is_parameterized_upsert():
    # WHY: user rows must UPSERT by username (reset password), never duplicate,
    # and use bound params so a username can't inject SQL at the login gate.
    sql = db.UPSERT_USER_SQL.lower()
    assert "insert into users" in sql
    assert "on conflict (username) do update" in sql
    assert "%s" in sql
    assert "{" not in db.UPSERT_USER_SQL and "format" not in db.UPSERT_USER_SQL


def test_month_options_range_boundaries():
    opts = db.month_options(today=date(2026, 1, 15))
    assert opts[0] == "2025-01"          # seed start
    assert opts[-1] == "2027-01"         # today + 12 months
    assert "2026-01" in opts
    assert opts == sorted(opts)          # ascending
    assert len(opts) == len(set(opts))   # no dupes


import pytest


def _pg_up():
    try:
        c = db.get_connection()
        c.close()
        return True
    except Exception:
        return False


live = pytest.mark.skipif(not _pg_up(), reason="Postgres not running")


@live
def test_upsert_then_load_roundtrip():
    db.upsert_macro("2099-12", 999.0, 1.5)     # add
    m = db.load_macro()
    row = m[m["month"] == "2099-12"].iloc[0]
    assert row["cpi"] == 999.0 and row["rate"] == 1.5
    db.upsert_macro("2099-12", 111.0, 2.5)     # edit in place
    m2 = db.load_macro()
    assert (m2["month"] == "2099-12").sum() == 1          # no duplicate row
    assert m2[m2["month"] == "2099-12"].iloc[0]["cpi"] == 111.0


@live
def test_list_batches_columns():
    b = db.list_batches()
    assert list(b.columns) == ["model_version", "scored_at"]


def test_hash_password_roundtrips_and_salts():
    # WHY: login must accept the right password and reject wrong ones; distinct
    # salts mean identical passwords never share a hash (no rainbow-table reuse).
    import bcrypt
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dashboard"))
    import auth
    h = auth.hash_password("s3cret-pw")
    assert bcrypt.checkpw(b"s3cret-pw", h.encode())
    assert not bcrypt.checkpw(b"wrong", h.encode())
    assert h != auth.hash_password("s3cret-pw")   # random salt each time
