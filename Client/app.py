import streamlit as st
from streamlit_option_menu import option_menu
from streamlit_autorefresh import st_autorefresh
import requests
import time
from style import apply_style, draw_metric

st.set_page_config(page_title="PyKV ", layout="wide")
BASES = [
    "http://127.0.0.1:8000", 
    "http://127.0.0.1:8001",
    "http://127.0.0.1:8002"
]

apply_style()

# @st.cache_data(ttl=5)
def get_active_base():
    for base in BASES:
        try:
            requests.get(f"{base}/stats", timeout=0.5)
            return base
        except:
            continue
    st.error("No backend available")
    return None

# ---------------- AUTO REFRESH ----------------
st_autorefresh(interval=2000, limit=None, key="refresh_counter")  # refresh every 2s

BASE = get_active_base()
if not BASE:
    st.error("❌ No PyKV server available")
    st.stop()

# ---------------- SIDEBAR ----------------
with st.sidebar:
    # Title matching MediChain's style
    st.markdown('<div class="sidebar-title">✦ PyKV Store</div>', unsafe_allow_html=True)
    st.markdown('<div class="sidebar-divider"></div>', unsafe_allow_html=True)

    selected = option_menu(
        menu_title=None, # Required
        options=["Dashboard", "Key Operations"],
        # Match the icons from your 2nd image (grid and key)
        icons=["grid-fill", "key-fill"], 
        default_index=0,
        styles={
            "container": {
                "padding": "0px 2px !important", 
                "background-color": " #34548a !important",
                "border-radius" : "0px"
            },
            "icon": {
                "color": "white", 
                "font-size": "22px",
                "margin-right": "10px"
            }, 
            "nav-link": {
                "color": "white", 
                "font-size": "15px", 
                "text-align": "left", 
                "margin": "10px 0px", 
                "padding": "12px 15px",
                "border-radius": "10px",
                "font-family": "'Inter', sans-serif"
            },
            "nav-link-selected": {
                "background-color": "#4383f1", # Brighter blue for selection
                "font-weight": "600"
            },
        }
    )


# ---------------- DASHBOARD ----------------
if selected == "Dashboard":
    st.markdown("<h1 style='text-align:center;color:#0e7c86;margin-top:-3rem;'>📊 PyKV Dashboard</h1>", unsafe_allow_html=True)

    try:
        stats = requests.get(f"{BASE}/stats", timeout=1).json()
    except Exception:
        st.warning("Switching to replica...")
        st.cache_data.clear()
        st.rerun()
        # stats = {}
    
    hits = 0
    misses = 0
    display_stats = {}

    hits = stats.get("hits", 0)
    misses = stats.get("misses", 0)
    total = hits + misses
    hit_ratio = round((hits / total) * 100, 2) if total else 0

    # Cache Metrics
    c1, c2, c3, c4, c5 = st.columns(5)
    

    with c1: draw_metric("Capacity", stats.get("capacity", "N/A"), "Total registered slots", "👥")
    with c2: draw_metric("Current Size", stats.get("size", "N/A"), "Keys with results", "✅")
    with c3: draw_metric("Evictions", stats.get("evictions", "N/A"), "Cache removals", "🗑️")
    with c4: draw_metric("Hits", hits, "Successful lookups", "🎯")
    with c5: draw_metric("Misses", misses, "Failed lookups", "❌")

    st.markdown("<br>", unsafe_allow_html=True)
    
    # Wrap in a container to manage spacing
    with st.container():
        # Using a styled caption for better visibility
        st.markdown(f"""
            <div style="margin-bottom: 5px; color: #1e293b; font-weight: 600; font-size: 14px;">
                Hit Ratio: {hit_ratio}%
            </div>
        """, unsafe_allow_html=True)
        
        st.progress(hit_ratio / 100)
    
    st.markdown(
    "<hr style='border: 1px solid gray;'>",
    unsafe_allow_html=True
    )


    # -------- Replication Health --------
    st.markdown("### 🔁 Healthy Replicas")
    try:
        r = requests.get(f"{BASE}/replication/health", timeout=1)
        if r.status_code == 200:
            replicas = r.json().get("healthy", [])
        # replicas = r.json().get("healthy", [])
        if replicas:
            for rep in replicas:
                st.markdown(f"""
                <div style="background-color: #dcfce7; color: #1a1a1a; padding: 12px; border-radius: 8px; border-left: 5px solid #22c55e; margin-bottom: 10px; font-size: 14px; font-weight: 500;">
                    🟢 Replica : <a href="{rep}" style="color: #1a1a1a; text-decoration: underline;">{rep}</a>
                </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div style="background-color: #fff9e6; color: #d97706; padding: 12px; border-radius: 8px; border-left: 5px solid #f59e0b; font-size: 14px;">
            ⚠️ No healthy replicas available
            </div>
        """, unsafe_allow_html=True)
    except:
        st.info("Replication info unavailable")
    
    st.markdown(
    "<hr style='border: 1px solid gray;'>",
    unsafe_allow_html=True
    )

    # --- STORED KEYS (Unified White Table Card) ---
    st.markdown("### 🔍 Stored Keys")

    search = st.text_input("Search Key", label_visibility="collapsed", placeholder="Enter key name to search...")

    try:
        r = requests.get(f"{BASE}/kv-items", timeout=2)
        data = r.json().get("items", {}) if r.status_code == 200 else {}
        
        if search:
            data = {k: v for k, v in data.items() if search.lower() in k.lower()}

        if data:
            
            # Table Headers
            st.markdown("""
                <div class="table-header">
                    <span style="flex: 1;">Key Name</span>
                    <span style="flex: 2; text-align: center;">Stored Value</span>
                    <span style="flex: 1; text-align: right;">Time To Live</span>
                </div>
            """, unsafe_allow_html=True)

            # Loop through keys
            for key, info in data.items():
                ttl_text = "∞" if info["ttl"] is None else f"{info['ttl']}s"
                st.markdown(f"""
                    <div class="table-row">
                        <div class="row-key">🔑 {key}</div>
                        <div class="row-value">"{info['value']}"</div>
                        <div class="row-ttl">{ttl_text}</div>
                    </div>
                """, unsafe_allow_html=True)
        
        else:
            st.info("No keys found in storage.")
    except:
        st.error("Backend not reachable.")



# ---------------- KEY OPERATIONS ----------------
elif selected == "Key Operations":
    st.markdown("<h2 style='color:#0e7c86;'>🔑 Key Operations</h2>", unsafe_allow_html=True)
    
    with st.container():
        tab1, tab2, tab3, tab4 = st.tabs(["SET", "GET", "UPDATE", "DELETE"])

        with tab1:
            key = st.text_input("Key", key="final_set_key")
            value = st.text_input("Value", key="final_set_value")
            ttl = st.number_input("TTL (seconds, optional)", min_value=0, step=1, value=0, key="final_set_ttl" )
            ttl = ttl if ttl > 0 else None
            
            st.markdown('<div class="op-button">', unsafe_allow_html=True)
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
            st.markdown('</div>', unsafe_allow_html=True)

        with tab2:
            key = st.text_input("Key to Fetch", key="final_get_key")
            st.markdown('<div class="op-button">', unsafe_allow_html=True)
            if st.button("GET"):
                start = time.time()
                r = requests.get(f"{BASE}/kv/{key}")
                latency = round((time.time() - start) * 1000, 2)

                if r.status_code in (200, 201):
                    st.success(f"Value: {r.json().get('value')} (Latency: {latency} ms)")
                else:
                    st.error("Key not found")

            st.markdown('</div>', unsafe_allow_html=True)

        with tab3:
            key = st.text_input("Key to Update", key="final_update_key")
            value = st.text_input("New Value", key="final_update_value")
            ttl = st.number_input("TTL (seconds, optional)", min_value=0, step=1, value=0, key="final_update_ttl")

            ttl = ttl if ttl > 0 else None
            st.markdown('<div class="op-button">', unsafe_allow_html=True)
            if st.button("UPDATE"):
                payload = {"value": value}
                if ttl:
                    payload["ttl"] = ttl
                r = requests.put(f"{BASE}/kv/{key}", json=payload)
                if r.status_code in (200, 201):
                    st.success("Key updated successfully")
                else:
                    st.error("Update failed")
            st.markdown('</div>', unsafe_allow_html=True)

        with tab4:
            key = st.text_input("Key to Delete", key="final_delete_key")
            confirm = st.checkbox("Confirm delete")

            st.markdown('<div class="op-button">', unsafe_allow_html=True)
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
            st.markdown('</div>', unsafe_allow_html=True)
