import streamlit as st
import requests

API_BASE = "http://127.0.0.1:8000"

def parse_pydantic_error(resp):
    try:
        errors = resp.json().get("detail", [])
        if not errors:
            return "Invalid input. Please check all fields."

        field = errors[0]["loc"][-1]
        msg = errors[0]["msg"]

        if field == "email":
            return "Invalid email address. Please enter a valid email."

        if field == "password":
            if "at least" in msg:
                return "Password must be at least 8 characters long"
            return "Invalid password."

        if field == "username":
            return "Invalid username."

        return "Invalid input. Please check all fields."

    except Exception:
        return "Invalid input. Please check all fields."

    
def auth_page():
    main_container = st.container(
        key = "form",
        horizontal_alignment="center",
        vertical_alignment="center"
    )

    with main_container: 
        content_container = st.container(
            width = 500,
            gap = "medium"
    )
    
    with content_container:
        field_container = st.container()
        
    with field_container:

        st.markdown("<h1 style='text-align: center; color: #34548a; margin-bottom: 0;'>✦ PyKV</h1>", unsafe_allow_html=True)
        st.markdown("<p style='text-align: center; color: #64748b; font-size: 14px;'>Distributed Key-Value Store Dashboard</p>", unsafe_allow_html=True)
        
        # Reference-style Tabs (Login and Register)
        tab_login, tab_register = st.tabs(["Login", "Register"])
        
        with tab_login:
            username = st.text_input("Username", placeholder="Enter your username", key="login_user")
            password = st.text_input("Password", type="password", placeholder="Enter your password", key="login_pass")
            
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("Login", use_container_width=True):
                if not username or not password:
                    st.error("Both fields are required")
                    st.stop()

                r = requests.post(
                    f"{API_BASE}/auth/login",
                    json={"username": username, "password": password}
                )

                if r.status_code == 200:
                    st.session_state["logged_in"] = True
                    st.session_state["username"] = username
                    st.query_params["user"] = username
                    st.success("Login successful")
                    st.rerun()
                else:
                    st.error(r.json().get("detail", "Invalid username or password"))


        with tab_register:
            new_user = st.text_input("Username", placeholder="Enter your username", key="reg_user")
            new_email = st.text_input("Email", placeholder="Enter your Email", key="reg_email")
            new_pass = st.text_input("Password", type="password", placeholder="Enter your password", key="reg_pass")
            conf_pass = st.text_input("Confirm Password", type="password", placeholder="Enter your password", key="reg_conf")
            
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("Create Account", use_container_width=True):

                if not new_user or not new_email or not new_pass or not conf_pass:
                    st.error("All fields are required")
                    st.stop()

                if new_pass != conf_pass:
                    st.error("Passwords do not match")
                    st.stop()

                r = requests.post(
                    f"{API_BASE}/auth/register",
                    json={
                        "username": new_user,
                        "email": new_email,
                        "password": new_pass
                    }
                )

                if r.status_code in (200, 201):
                    st.success("Account created. Please login.")
                elif r.status_code == 422:
                    st.error(parse_pydantic_error(r))
                else:
                    st.error(r.json().get("detail", "Registration failed"))