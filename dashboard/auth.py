"""Login gate for the dashboards.

Public Cloudflare tunnel = anyone with the URL can reach the app, so every
page must call `require_login()` before rendering. Passwords are bcrypt-hashed
in the `users` table (see sql/init.sql); this module never stores plaintext.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
import bcrypt  # noqa: E402
import streamlit as st  # noqa: E402
import db  # noqa: E402


def hash_password(plaintext):
    """bcrypt hash (str) for a plaintext password — used by manage_users.py."""
    return bcrypt.hashpw(plaintext.encode(), bcrypt.gensalt()).decode()


def verify(username, password):
    """True iff `username` exists and `password` matches its stored bcrypt hash."""
    stored = db.get_password_hash(username)
    if stored is None:
        return False
    return bcrypt.checkpw(password.encode(), stored.encode())


def require_login():
    """Block the page until the visitor logs in. Call right after set_page_config."""
    if st.session_state.get("auth_user"):
        with st.sidebar:
            st.caption(f"👤 {st.session_state['auth_user']}")
            if st.button("Đăng xuất"):
                del st.session_state["auth_user"]
                st.rerun()
        return

    st.title("🔒 Đăng nhập")
    with st.form("login"):
        username = st.text_input("Tên đăng nhập")
        password = st.text_input("Mật khẩu", type="password")
        submitted = st.form_submit_button("Đăng nhập")
    if submitted:
        try:
            ok = verify(username, password)
        except Exception as e:
            st.error(f"Không kết nối được Postgres: {e}")
            st.stop()
        if ok:
            st.session_state["auth_user"] = username
            st.rerun()
        else:
            st.error("Sai tên đăng nhập hoặc mật khẩu.")
    st.stop()
