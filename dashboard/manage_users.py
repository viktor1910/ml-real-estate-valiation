"""Create or reset a dashboard login. Run once to seed the first user.

    python dashboard/manage_users.py <username>

Prompts for a password (hidden), bcrypt-hashes it, and UPSERTs the user.
Re-running with an existing username resets that user's password.
"""
import getpass
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
import auth
import db


def main():
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(1)
    username = sys.argv[1].strip()
    if not username:
        print("Tên đăng nhập rỗng.")
        sys.exit(1)

    pw = getpass.getpass("Mật khẩu: ")
    if len(pw) < 8:
        print("Mật khẩu phải >= 8 ký tự.")
        sys.exit(1)
    if pw != getpass.getpass("Nhập lại mật khẩu: "):
        print("Mật khẩu nhập lại không khớp.")
        sys.exit(1)

    db.upsert_user(username, auth.hash_password(pw))
    print(f"Đã lưu user '{username}'.")


if __name__ == "__main__":
    main()
