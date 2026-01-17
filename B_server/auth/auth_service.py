from .auth_db import get_conn
import bcrypt

def hash_password(password):
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

def verify_password(password, hashed):
    return bcrypt.checkpw(password.encode(), hashed.encode())

def register_user(username, email, password):
    if len(password) < 8:
        return False, "Password must be at least 8 characters long"
    
    try:
        conn = get_conn()
        cursor = conn.cursor()

        cursor.execute(
            "INSERT INTO users (username, email, password) VALUES (?, ?, ?)",
            (username, email, hash_password(password))
        )

        conn.commit()
        conn.close()
        return True, "User registered"

    except Exception as e:
        if "UNIQUE" in str(e):
            return False, "Username or email already exists"
        return False, "Registration failed"


def login_user(username, password):
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT password FROM users WHERE username=?",
        (username,)
    )
    row = cursor.fetchone()
    conn.close()

    if not row:
        return False, "User not found"

    if not verify_password(password, row[0]):
        return False, "Wrong password"

    return True, "Login success"
