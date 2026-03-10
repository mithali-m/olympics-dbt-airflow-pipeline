import streamlit as st
import sys
from pathlib import Path

from utils.styles import apply_theme
apply_theme()

sys.path.append(str(Path(__file__).parent.parent))
from snowflake_db import run_query

st.set_page_config(page_title="Countries", page_icon="🌍", layout="wide")

st.title("🌍 Countries")
st.markdown("### Explore medal counts by country")

# ── Top Countries ──
st.subheader("🏆 Medal Table")
countries_df = run_query("""
    SELECT
        c.COUNTRY_NAME,
        f.COUNTRY_TOTAL_GOLD        AS GOLD,
        f.COUNTRY_TOTAL_MEDALS - f.COUNTRY_TOTAL_GOLD AS OTHER_MEDALS,
        f.COUNTRY_TOTAL_MEDALS      AS TOTAL_MEDALS,
        f.COUNTRY_BEST_RANK         AS BEST_RANK,
        c.COUNTRY_FIRST_PARTICIPATION AS FIRST_PARTICIPATION
    FROM OLYMPICS_DB.ANALYTICS.FACT_COUNTRY_PERFORMANCE f
    JOIN OLYMPICS_DB.ANALYTICS.DIM_COUNTRY c ON f.COUNTRY_SK = c.COUNTRY_SK
    ORDER BY TOTAL_MEDALS DESC
""")
st.dataframe(countries_df, use_container_width=True)

st.divider()

# ── Country Search ──
st.subheader("🔍 Search a Country")
countries = countries_df["COUNTRY_NAME"].tolist()
selected_country = st.selectbox("Select a country", countries)

if selected_country:
    country_data = countries_df[countries_df["COUNTRY_NAME"] == selected_country].iloc[0]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("🥇 Gold Medals", country_data["GOLD"])
    col2.metric("🏅 Total Medals", country_data["TOTAL_MEDALS"])
    col3.metric("🏆 Best Rank", country_data["BEST_RANK"])
    col4.metric("📅 First Participation", country_data["FIRST_PARTICIPATION"])

    st.divider()

    # Athletes from this country
    st.subheader(f"🏃 Athletes from {selected_country}")
    athletes_df = run_query(f"""
        SELECT
            a.ATHLETE_NAME,
            a.GENDER,
            a.DATE_OF_BIRTH,
            a.IS_RECORD_HOLDER,
            p.TOTAL_MEDALS_WON,
            p.GOLD_MEDALS,
            p.TOTAL_OLYMPICS_ATTENDED
        FROM OLYMPICS_DB.ANALYTICS.DIM_ATHLETE a
        JOIN OLYMPICS_DB.ANALYTICS.FACT_ATHLETE_PERFORMANCE p ON a.ATHLETE_SK = p.ATHLETE_SK
        JOIN OLYMPICS_DB.ANALYTICS.DIM_COUNTRY c ON c.COUNTRY_NAME = '{selected_country}'
        JOIN OLYMPICS_DB.ANALYTICS.FACT_EVENT_PERFORMANCE e ON e.ATHLETE_SK = a.ATHLETE_SK
            AND e.COUNTRY_SK = c.COUNTRY_SK
        GROUP BY
            a.ATHLETE_NAME, a.GENDER, a.DATE_OF_BIRTH,
            a.IS_RECORD_HOLDER, p.TOTAL_MEDALS_WON,
            p.GOLD_MEDALS, p.TOTAL_OLYMPICS_ATTENDED
        ORDER BY p.TOTAL_MEDALS_WON DESC
    """)
    st.dataframe(athletes_df, use_container_width=True)