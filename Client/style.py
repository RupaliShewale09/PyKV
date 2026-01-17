import streamlit as st

def apply_style():
        st.markdown("""
    <style>
        /* Global Background */
        .stApp { background-color: #f8fafd; }
        #MainMenu, footer, header {visibility: hidden;}
        .block-container { padding-top: 2rem; }

        /* Sidebar Fix */
        [data-testid="stSidebar"] {
            background-color: #34548a !important;
            width: 280px !important;
        }

        /*  Remove the dark inner box/container default styling */
        [data-testid="stSidebar"] div[data-testid="stVerticalBlock"] > div {
            background-color: transparent !important;
        }
                
        div[data-testid="stMarkdownContainer"] p {
            color: #000000 !important; /* Force labels to Black */
        }
                
        .sidebar-title {
            color: white;
            font-family: 'Inter', sans-serif;
            font-size: 24px;
            font-weight: 700;
            padding: 0px 0px 20px 20px;
            letter-spacing: 0.5px;
        }
        
        /* Short accent line under the title */
        .sidebar-divider {
            height: 1.5px;
            background-color: #4383f1;
            margin-bottom: 20px;
            border-radius: 2px;
        }
        
        .metric-card-container {
            background-color: white; 
            padding: 20px; 
            border-radius: 12px; 
            box-shadow: 0 4px 6px rgba(0,0,0,0.05); 
            border: 1px solid #eef2f6;
            height: 160px; /* FIXED HEIGHT for uniform sizing */
            display: flex;
            flex-direction: column;
            justify-content: space-between;
        }
                
        .section-card {
            background-color: white;
            padding: 25px;
            border-radius: 12px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.05);
            border: 1px solid #eef2f6;
            margin-bottom: 20px;
        }
        
        .stProgress > div > div > div > div {
            background-color: #4383f1 !important; /* Blue progress bar */
        }
        .stCaption {
            color: #1e293b !important; /* Dark blue/gray text */
            font-weight: 600 !important;
            font-size: 14px !important;
        }
        
        .stTextInput > div > div > input {
            background-color: white !important;
            color: #1e293b !important;
            border: 1px solid #eef2f6 !important;
            border-radius: 8px !important;
            padding: 10px !important;
        }
        
        .stTextInput > div > div > input::placeholder {
            color: #94a3b8 !important; /* Grayish blue that is visible */
            opacity: 1 !important;    /* Ensure it's not transparent */
        }

        /* Table Header */
        .table-header {
            display: flex;
            justify-content: space-between;
            padding: 12px 20px;
            background-color: #f8fafc;
            border-bottom: 2px solid #f1f5f9;
            font-weight: 700;
            color: #64748b;
            font-size: 17px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        /* Individual Row */
        .table-row {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 15px 20px;
            border-bottom: 1px solid #f1f5f9;
            transition: background-color 0.2s;
        }

        .table-row:hover {
            background-color: #fcfdfe;
        }

        .row-key { color: #1e293b; font-weight: 600; flex: 1; }
        .row-value { color: #4383f1; font-family: 'Courier New', monospace; font-weight: 600; flex: 2; text-align: center; }
        .row-ttl { color: #94a3b8; font-size: 12px; flex: 1; text-align: right; }
        
        /* Header visibility fixes */
        h1, h2, h3 { color: #1e293b !important; font-family: 'Inter', sans-serif; margin-bottom: 15px !important; }
        label, .stMarkdown p { color: #000000 !important; font-weight: 600 !important; }    
        
        /* Force Tabs to have dark text and blue underline */
        .stTabs [data-baseweb="tab-list"] button [data-testid="stMarkdownContainer"] p {
            color: #1e293b !important;
            font-weight: 600 !important;
        }
        
        .stTabs [data-baseweb="tab-highlight"] {
            background-color: #4383f1 !important;
        }

        /* Style number input and text input borders to be clean white/gray */
        .stNumberInput div div, .stTextInput div div {
            background-color: white !important;
            border: 1px solid #eef2f6 !important;
            color: #1e293b !important;
        }
        
        div[data-testid="stNumberInput"] div[data-baseweb="input"] {
            background-color: white !important;
            color: black !important;
        }

        div[data-testid="stNumberInput"] input {
            color: black !important;
            -webkit-text-fill-color: black !important;
        }
                    
        div.stButton > button {
            background-color: #4383f1 !important; 
            color: #ffffff !important;           
            border-radius: 8px !important;
            border: 2px solid #4383f1 !important; /* Border to keep size same on hover */
            padding: 10px 25px !important;
            font-weight: 600 !important;
            transition: all 0.3s ease-in-out !important;
        }
        div.stButton > button p {
            color: #ffffff !important;
        }
        div.stButton > button:hover p {
            color: #000000 !important; /* Changes text to black when background turns white on hover */
        }
                
        div.stButton > button:hover {
            background-color: #ffffff !important; 
            color: #000000 !important;            
            border: 2px solid #000000 !important; /* Black border on hover */
        }

        div[data-testid="stNumberInput"] button {
            background-color: #f8fafc !important;
            color: #000000 !important;
            border: 1px solid #eef2f6 !important;
        }

        .st-key-form {
            background-color: white !important;
            padding: 40px !important;
            border-radius: 15px !important;
            box-shadow: 0 10px 25px rgba(0,0,0,0.1) !important;
            border: 1px solid #eef2f6 !important;
            max-width: 550px;
            margin: auto;
        }
            div[data-testid="stTextInput"] div[data-baseweb="input"] {
            background-color: white ;
        }
        /* Force password visibility toggle icon to dark */
        div[data-testid="stTextInput"] svg {
            fill: #64748b !important;   /* Dark gray color for visibility */
            stroke: #64748b !important; /* Sometimes needed for SVG stroke */
        }

    </style>
    """, unsafe_allow_html=True)

def draw_metric(label, value, subtext, icon="📊"):
    st.markdown(f"""
    <div class="metric-card-container">
        <div style="display: flex; justify-content: space-between; align-items: center;">
            <span style="color: #6c757d; font-size: 13px; font-weight: 600;">{label}</span>
            <span style="background: #f1f5f9; padding: 5px; border-radius: 6px;">{icon}</span>
        </div>
        <div>
            <div style="color: #1a1a1a; font-size: 24px; font-weight: 700; margin: 5px 0;">{value}</div>
            <div style="color: #94a3b8; font-size: 11px;">{subtext}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)
