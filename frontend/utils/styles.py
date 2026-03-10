import streamlit as st

def apply_theme():
    st.markdown("""
        <style>
            .stApp { background-color: #1E3A4F; }
            [data-testid="stSidebar"] { background-color: #162D3E; }
            html, body, [class*="css"], .stMarkdown, .stText, p, li { color: white !important; }
            h1, h2 { color: white !important; font-weight: 700; }
            h3 { color: #c8e0e6 !important; }
            [data-testid="stDataFrame"] { background-color: #162D3E !important; }
            [data-testid="metric-container"] {
                background-color: #162D3E;
                border-left: 4px solid #5B9EC9;
                border-radius: 8px;
                padding: 12px;
            }
            .stSelectbox > div, .stTextInput > div > div {
                background-color: #162D3E !important;
                color: white !important;
                border-color: #5B9EC9 !important;
            }
            .stButton > button {
                background-color: #5B9EC9;
                color: white;
                border: none;
                border-radius: 6px;
            }
            .stButton > button:hover { background-color: #4A8DB8; }
            [data-testid="stSidebar"] * { color: white !important; }
        </style>
    """, unsafe_allow_html=True)