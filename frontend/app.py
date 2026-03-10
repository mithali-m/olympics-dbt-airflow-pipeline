import streamlit as st

st.set_page_config(
    page_title="Olympics Data Explorer",
    page_icon="🏅",
    layout="wide"
)

st.markdown("""
    <style>
        /* Main background */
        .stApp {
            background-color: #1E3A4F;
        }

        /* Sidebar background */
        [data-testid="stSidebar"] {
            background-color: #162D3E;
        }

        /* All text white */
        html, body, [class*="css"], .stMarkdown, .stText {
            color: white !important;
        }

        /* Title styling */
        h1 {
            color: white !important;
            font-weight: 700;
        }

        /* Subheader / h3 */
        h3 {
            color: #5B9EC9 !important;
        }

        /* Card-style container for the bullet list */
        .info-card {
            background-color: #162D3E;
            border-left: 4px solid #5B9EC9;
            border-radius: 8px;
            padding: 20px 28px;
            margin-top: 16px;
            color: white;
            font-size: 16px;
            line-height: 2;
        }
    </style>
""", unsafe_allow_html=True)

st.title("🏅 Olympics Data Explorer")
st.markdown("### Explore 128 years of Olympic history from 1896 to 2024")

st.markdown("""
<div class="info-card">
    Use the pages on the left to explore:<br><br>
    🌍 <b>Countries</b> — Medal counts by country<br>
    🏃 <b>Athletes</b> — Search and explore athletes<br>
    🏆 <b>Events</b> — Explore sports and events<br>
    📊 <b>Overview</b> — Key statistics at a glance
</div>
""", unsafe_allow_html=True)