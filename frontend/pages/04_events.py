import streamlit as st
import sys
from pathlib import Path

from utils.styles import apply_theme
apply_theme()

sys.path.append(str(Path(__file__).parent.parent))
from snowflake_db import run_query

st.set_page_config(page_title="Events", page_icon="🏆", layout="wide")

st.title("🏆 Events")
st.markdown("### Explore Olympic sports and events")

# ── Filters ──
col1, col2 = st.columns(2)

with col1:
    sports_df = run_query("SELECT DISTINCT SPORT FROM OLYMPICS_DB.ANALYTICS.DIM_SPORT ORDER BY SPORT")
    selected_sport = st.selectbox("🏅 Select a Sport", ["All"] + sports_df["SPORT"].tolist())

with col2:
    hosts_df = run_query("SELECT DISTINCT YEAR, HOST_CITY FROM OLYMPICS_DB.ANALYTICS.DIM_HOST ORDER BY YEAR DESC")
    host_options = ["All"] + [f"{row['YEAR']} - {row['HOST_CITY']}" for _, row in hosts_df.iterrows()]
    selected_host = st.selectbox("📅 Select Olympics Year", host_options)

st.divider()

# ── Build filters ──
sport_filter = f"AND sp.SPORT = '{selected_sport}'" if selected_sport != "All" else ""

if selected_host != "All":
    selected_year = selected_host.split(" - ")[0]
    host_filter = f"AND h.YEAR = {selected_year}"
else:
    host_filter = ""

# ── Events Table ──
st.subheader("🎯 Event Results")
events_df = run_query(f"""
    SELECT
        h.YEAR,
        h.HOST_CITY,
        sp.SPORT,
        sp.GAMES_TYPE,
        ev.EVENT,
        ev.TEAM_OR_INDIVIDUAL,
        a.ATHLETE_NAME,
        c.COUNTRY_NAME,
        m.MEDAL,
        f.RESULT_VALUE,
        sp.RESULT_UNIT
    FROM OLYMPICS_DB.ANALYTICS.FACT_EVENT_PERFORMANCE f
    JOIN OLYMPICS_DB.ANALYTICS.DIM_ATHLETE a ON f.ATHLETE_SK = a.ATHLETE_SK
    JOIN OLYMPICS_DB.ANALYTICS.DIM_COUNTRY c ON f.COUNTRY_SK = c.COUNTRY_SK
    JOIN OLYMPICS_DB.ANALYTICS.DIM_HOST h ON f.HOST_SK = h.HOST_SK
    JOIN OLYMPICS_DB.ANALYTICS.DIM_SPORT sp ON f.SPORT_SK = sp.SPORT_SK
    JOIN OLYMPICS_DB.ANALYTICS.DIM_EVENT ev ON f.EVENT_SK = ev.EVENT_SK
    JOIN OLYMPICS_DB.ANALYTICS.DIM_MEDALS m ON f.MEDAL_SK = m.MEDAL_SK
    WHERE m.MEDAL != 'No Medal'
    {sport_filter}
    {host_filter}
    ORDER BY h.YEAR DESC, sp.SPORT, m.MEDAL
    LIMIT 500
""")

if events_df.empty:
    st.warning("No results found for selected filters")
else:
    st.success(f"Showing {len(events_df)} results")
    st.dataframe(events_df, use_container_width=True)

st.divider()

# ── Sports Summary ──
st.subheader("📊 Athletes per Sport")
sport_summary = run_query("""
    SELECT
        sp.SPORT,
        sp.GAMES_TYPE,
        COUNT(DISTINCT f.ATHLETE_SK)    AS TOTAL_ATHLETES,
        COUNT(DISTINCT ev.EVENT_SK)     AS TOTAL_EVENTS,
        SUM(CASE WHEN m.MEDAL != 'No Medal' THEN 1 ELSE 0 END) AS TOTAL_MEDALS
    FROM OLYMPICS_DB.ANALYTICS.FACT_EVENT_PERFORMANCE f
    JOIN OLYMPICS_DB.ANALYTICS.DIM_SPORT sp ON f.SPORT_SK = sp.SPORT_SK
    JOIN OLYMPICS_DB.ANALYTICS.DIM_EVENT ev ON f.EVENT_SK = ev.EVENT_SK
    JOIN OLYMPICS_DB.ANALYTICS.DIM_MEDALS m ON f.MEDAL_SK = m.MEDAL_SK
    GROUP BY sp.SPORT, sp.GAMES_TYPE
    ORDER BY TOTAL_ATHLETES DESC
""")
st.dataframe(sport_summary, use_container_width=True)