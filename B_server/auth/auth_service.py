from .auth_db import get_conn, User
import bcrypt
from sqlalchemy.exc import IntegrityError

def hash_password(password):
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

def verify_password(password, hashed):
    return bcrypt.checkpw(password.encode(), hashed.encode())

def register_user(username, email, password):
    
    session = get_conn()
    try:
        new_user = User(
            username=username,
            email=email,
            password=hash_password(password)
        )
        session.add(new_user)
        session.commit()
        return True, "User registered"
    
    except IntegrityError as e:
        session.rollback()
        return False, "Username or email already exists"
    
    except Exception as e:
        session.rollback()
        return False, "Registration failed"
    
    finally:
        session.close()

def login_user(username, password):
    session = get_conn()
    try:
        user = session.query(User).filter(User.username == username).first()
        if not user:
            return False, "User not found"
        if not verify_password(password, user.password):
            return False, "Wrong password"
        return True, "Login success"
    finally:
        session.close()
