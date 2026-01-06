import streamlit as st
from streamlit_option_menu import option_menu
from streamlit_autorefresh import st_autorefresh
import requests
import time

st.set_page_config(page_title="PyKV ", layout="wide")
BASES = [
    "http://127.0.0.1:8000", 
    "http://127.0.0.1:8001"
]

def get_active_base():
    for base in BASES:
        try:
            requests.get(f"{base}/stats", timeout=1)
            return base
        except:
            continue
    st.error("No backend available")
    return None

# ---------------- STYLING ----------------
st.markdown("""
<style>
#MainMenu, footer, header {visibility: hidden;}
            
main > div:first-child {
    padding-top: 0rem;  /* default is ~5rem */
}
            
.stButton>button {
    background-color: #0e7c86;
    color: white;
    font-weight: bold;
    padding: 8px 20px;
    border-radius: 10px;
}
.stButton>button:hover {
    background-color: #095f66;
}
</style>
""", unsafe_allow_html=True)

# ---------------- AUTO REFRESH ----------------
st_autorefresh(interval=2000, limit=None, key="refresh_counter")  # refresh every 2s

BASE = get_active_base()
if not BASE:
    st.error("❌ No PyKV server available")
    st.stop()

# ---------------- SIDEBAR ----------------
with st.sidebar:
    selected = option_menu(
        menu_title="PyKV Store",
        options=["Dashboard", "Key Operations"],
        icons=["speedometer2", "key"],
        default_index=0,
        styles={
            "nav-link-selected": {"background-color": "#0e7c86"},
            "icon": {"color": "orange", "font-size": "20px"},
        },
    )


# ---------------- DASHBOARD ----------------
if selected == "Dashboard":
    st.markdown("<h2 style='text-align:center;color:#0e7c86;margin-top:-6rem;'>📊 PyKV Dashboard</h2>", unsafe_allow_html=True)

    try:
        stats = requests.get(f"{BASE}/stats", timeout=2).json()
    except Exception:
        stats = {}

    hits = stats.get("hits", 0)
    misses = stats.get("misses", 0)
    total = hits + misses
    hit_ratio = round((hits / total) * 100, 2) if total else 0

    # Cache Metrics
    c1, c2, c3, c4, c5 = st.columns(5)

    c1.metric("Capacity", stats.get("capacity", "N/A"))
    c2.metric("Current Size", stats.get("size", "N/A"))
    c3.metric("Evictions", stats.get("evictions", "N/A"))
    c4.metric("Hits", hits)
    c5.metric("Misses", misses)

    st.progress(hit_ratio / 100)
    st.caption(f"Hit Ratio: {hit_ratio}%")

    # -------- Replication Health --------
    st.markdown("### 🔁 Replication Health")
    try:
        r = requests.get(f"{BASE}/replication/health", timeout=2)
        replicas = r.json().get("healthy", [])
        if replicas:
            for rep in replicas:
                st.success(f"🟢 {rep}")
        else:
            st.warning("No healthy replicas")
    except:
        st.info("Replication info unavailable")

    # Key Listing + Search
    st.markdown("### 🔍 Stored Keys")
    search = st.text_input("Search Key")
    try:
        r = requests.get(f"{BASE}/kv-items", timeout=2)
        data = r.json().get("items", {}) if r.status_code == 200 else {}

        if search:
            data = {
                k: v for k, v in data.items()
                if search.lower() in k.lower()
            }

        if data:
            for key, info in data.items():
                ttl = info["ttl"]
                ttl_text = "∞" if ttl is None else f"{ttl}s"
                st.code(f"🔑 {key} = \"{info['value']}\",     TTL: \"{ttl_text}\"")
        else:
            st.info("No keys found")
    except:
        st.error("Backend not reachable")



# ---------------- KEY OPERATIONS ----------------
elif selected == "Key Operations":
    st.markdown("<h2 style='color:#0e7c86;'>🔑 Key Operations</h2>", unsafe_allow_html=True)

    tab1, tab2, tab3, tab4 = st.tabs(["SET", "GET", "UPDATE", "DELETE"])

    with tab1:
        key = st.text_input("Key", key="set_key")
        value = st.text_input("Value", key="set_value")
        ttl = st.number_input("TTL (seconds, optional)", min_value=0, step=1, value=0, key="set_ttl" )
        ttl = ttl if ttl > 0 else None
        if st.button("SET"):
            payload = {"key": key, "value": value}
            if ttl:
                payload["ttl"] = ttl

            start = time.time()
            r = requests.post(f"{BASE}/kv/", json=payload)
            latency = round((time.time() - start) * 1000, 2)
            if r.status_code in (200, 201):
                st.success(f"Key inserted (Latency: {latency} ms)")
            else:
                st.error("Failed to insert key")

    with tab2:
        key = st.text_input("Key to Fetch", key="get_key")
        if st.button("GET"):
            start = time.time()
            r = requests.get(f"{BASE}/kv/{key}")
            latency = round((time.time() - start) * 1000, 2)

            if r.status_code in (200, 201):
                st.success(f"Value: {r.json().get('value')} (Latency: {latency} ms)")
            else:
                st.error("Key not found")

    with tab3:
        key = st.text_input("Key to Update", key="update_key")
        value = st.text_input("New Value", key="update_value")
        ttl = st.number_input("TTL (seconds, optional)", min_value=0, step=1, value=0, key="update_ttl")

        ttl = ttl if ttl > 0 else None
        if st.button("UPDATE"):
            payload = {"value": value}
            if ttl:
                payload["ttl"] = ttl
            r = requests.put(f"{BASE}/kv/{key}", json=payload)
            if r.status_code in (200, 201):
                st.success("Key updated successfully")
            else:
                st.error("Update failed")

    with tab4:
        key = st.text_input("Key to Delete", key="delete_key")
        confirm = st.checkbox("Confirm delete")
        if st.button("DELETE"):
            if not confirm:
                st.warning("Please confirm deletion")
            else:
                r = requests.delete(f"{BASE}/kv/{key}")
                if r.status_code == 204:
                    st.success("Key deleted successfully")
                elif r.status_code == 404:
                    st.warning("Key not found")
                else:
                    st.error("Delete failed")
