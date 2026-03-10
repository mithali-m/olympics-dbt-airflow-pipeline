import streamlit as st
import sys
from pathlib import Path
import plotly.express as px

from utils.styles import apply_theme
apply_theme()

sys.path.append(str(Path(__file__).parent.parent))
from snowflake_db import run_query

st.set_page_config(page_title="Overview", page_icon="📊", layout="wide")

st.title("📊 Overview")
st.markdown("### Key statistics at a glance")

# ── Key Metrics ──
col1, col2, col3, col4 = st.columns(4)

total_athletes = run_query("SELECT COUNT(DISTINCT ATHLETE_ID) FROM OLYMPICS_DB.ANALYTICS.DIM_ATHLETE")
total_countries = run_query("SELECT COUNT(*) FROM OLYMPICS_DB.ANALYTICS.DIM_COUNTRY")
total_sports = run_query("SELECT COUNT(DISTINCT SPORT) FROM OLYMPICS_DB.ANALYTICS.DIM_SPORT")
total_events = run_query("SELECT COUNT(*) FROM OLYMPICS_DB.ANALYTICS.DIM_EVENT")

col1.metric("🏃 Total Athletes", f"{total_athletes.iloc[0, 0]:,}")
col2.metric("🌍 Total Countries", f"{total_countries.iloc[0, 0]:,}")
col3.metric("🏅 Total Sports", f"{total_sports.iloc[0, 0]:,}")
col4.metric("🎯 Total Events", f"{total_events.iloc[0, 0]:,}")

st.divider()

# ── Medals Overview ──
st.subheader("🥇 Medal Distribution")
medals_df = run_query("""
    SELECT
        m.MEDAL,
        COUNT(*) AS TOTAL
    FROM OLYMPICS_DB.ANALYTICS.FACT_EVENT_PERFORMANCE f
    JOIN OLYMPICS_DB.ANALYTICS.DIM_MEDALS m ON f.MEDAL_SK = m.MEDAL_SK
    WHERE m.MEDAL != 'No Medal'
    GROUP BY m.MEDAL
    ORDER BY TOTAL DESC
""")
fig = px.bar(
    medals_df,
    x="MEDAL",
    y="TOTAL",
    color="MEDAL",
    color_discrete_map={
        "Gold": "#FFD700",
        "Silver": "#C0C0C0",
        "Bronze": "#CD7F32"
    }
)
fig.update_layout(xaxis_tickangle=0)
st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Top 10 Countries ──
st.subheader("🌍 Top 10 Countries by Total Medals")
top_countries = run_query("""
    SELECT
        c.COUNTRY_NAME,
        f.COUNTRY_TOTAL_GOLD        AS GOLD,
        f.COUNTRY_TOTAL_MEDALS      AS TOTAL_MEDALS
    FROM OLYMPICS_DB.ANALYTICS.FACT_COUNTRY_PERFORMANCE f
    JOIN OLYMPICS_DB.ANALYTICS.DIM_COUNTRY c ON f.COUNTRY_SK = c.COUNTRY_SK
    ORDER BY TOTAL_MEDALS DESC
    LIMIT 10
""")
st.dataframe(top_countries, use_container_width=True)

st.divider()

# ── Top 10 Sports by Athletes ──
st.subheader("🏅 Top 10 Sports by Number of Athletes")
top_sports = run_query("""
    SELECT
        sp.SPORT,
        COUNT(DISTINCT f.ATHLETE_SK) AS TOTAL_ATHLETES
    FROM OLYMPICS_DB.ANALYTICS.FACT_EVENT_PERFORMANCE f
    JOIN OLYMPICS_DB.ANALYTICS.DIM_SPORT sp ON f.SPORT_SK = sp.SPORT_SK
    GROUP BY sp.SPORT
    ORDER BY TOTAL_ATHLETES DESC
    LIMIT 10
""")

fig = px.bar(
    top_sports,
    x="TOTAL_ATHLETES",
    y="SPORT",
    orientation="h",
    color_discrete_sequence=["#5B9EC9"],
)

fig.update_layout(
    plot_bgcolor="#1E3A4F",
    paper_bgcolor="#1E3A4F",
    font_color="white",
    xaxis=dict(
        title="Number of Athletes",
        tickfont=dict(color="white"),
        gridcolor="#2E4A5F",
    ),
    yaxis=dict(
        title=None,
        tickfont=dict(color="white"),
        showgrid=False,
        autorange="reversed",   # Keeps highest value at the top
    ),
    margin=dict(t=20, l=150),   # Extra left margin for sport labels
    showlegend=False,
)

st.plotly_chart(fig, use_container_width=True)