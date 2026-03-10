import streamlit as st
import sys
from pathlib import Path

from utils.styles import apply_theme
apply_theme()

sys.path.append(str(Path(__file__).parent.parent))
from snowflake_db import run_query

st.set_page_config(page_title="Athletes", page_icon="🏃", layout="wide")

st.title("🏃 Athletes")
st.markdown("### Search and explore Olympic athletes")

# ── Search ──
search = st.text_input("🔍 Search athlete by name", placeholder="e.g. Michael Phelps")

if search:
    athletes_df = run_query(f"""
        SELECT
            a.ATHLETE_NAME,
            a.GENDER,
            a.DATE_OF_BIRTH,
            a.IS_RECORD_HOLDER,
            c.COUNTRY_NAME,
            p.AGE,
            p.HEIGHT_CM,
            p.WEIGHT_KG,
            p.TOTAL_OLYMPICS_ATTENDED,
            p.TOTAL_MEDALS_WON,
            p.GOLD_MEDALS,
            p.SILVER_MEDALS,
            p.BRONZE_MEDALS,
            co.COACH_NAME
        FROM OLYMPICS_DB.ANALYTICS.DIM_ATHLETE a
        JOIN OLYMPICS_DB.ANALYTICS.FACT_ATHLETE_PERFORMANCE p
            ON a.ATHLETE_SK = p.ATHLETE_SK
        JOIN OLYMPICS_DB.ANALYTICS.FACT_EVENT_PERFORMANCE e
            ON e.ATHLETE_SK = a.ATHLETE_SK
        JOIN OLYMPICS_DB.ANALYTICS.DIM_COUNTRY c
            ON e.COUNTRY_SK = c.COUNTRY_SK
        LEFT JOIN OLYMPICS_DB.ANALYTICS.DIM_COACH co
            ON p.COACH_SK = co.COACH_SK
        WHERE UPPER(a.ATHLETE_NAME) LIKE UPPER('%{search}%')
        GROUP BY
            a.ATHLETE_NAME, a.GENDER, a.DATE_OF_BIRTH,
            a.IS_RECORD_HOLDER, c.COUNTRY_NAME, p.AGE,
            p.HEIGHT_CM, p.WEIGHT_KG, p.TOTAL_OLYMPICS_ATTENDED,
            p.TOTAL_MEDALS_WON, p.GOLD_MEDALS, p.SILVER_MEDALS,
            p.BRONZE_MEDALS, co.COACH_NAME
        ORDER BY p.TOTAL_MEDALS_WON DESC
    """)

    if athletes_df.empty:
        st.warning(f"No athletes found for '{search}'")
    else:
        st.success(f"Found {len(athletes_df)} athlete(s)")
        st.dataframe(athletes_df, use_container_width=True)

        # ── Athlete Detail ──
        if len(athletes_df) == 1:
            athlete = athletes_df.iloc[0]
            st.divider()
            st.subheader(f"🏅 {athlete['ATHLETE_NAME']} — Career Stats")

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("🥇 Gold Medals", athlete["GOLD_MEDALS"])
            col2.metric("🏅 Total Medals", athlete["TOTAL_MEDALS_WON"])
            col3.metric("🎯 Olympics Attended", athlete["TOTAL_OLYMPICS_ATTENDED"])
            col4.metric("📏 Height", f"{athlete['HEIGHT_CM']} cm" if athlete["HEIGHT_CM"] else "N/A")

            st.divider()

            # Events the athlete competed in
            st.subheader("🎯 Events Competed In")
            events_df = run_query(f"""
                SELECT
                    h.YEAR,
                    h.HOST_CITY,
                    sp.SPORT,
                    ev.EVENT,
                    m.MEDAL,
                    f.RESULT_VALUE,
                    sp.RESULT_UNIT
                FROM OLYMPICS_DB.ANALYTICS.FACT_EVENT_PERFORMANCE f
                JOIN OLYMPICS_DB.ANALYTICS.DIM_ATHLETE a ON f.ATHLETE_SK = a.ATHLETE_SK
                JOIN OLYMPICS_DB.ANALYTICS.DIM_HOST h ON f.HOST_SK = h.HOST_SK
                JOIN OLYMPICS_DB.ANALYTICS.DIM_SPORT sp ON f.SPORT_SK = sp.SPORT_SK
                JOIN OLYMPICS_DB.ANALYTICS.DIM_EVENT ev ON f.EVENT_SK = ev.EVENT_SK
                JOIN OLYMPICS_DB.ANALYTICS.DIM_MEDALS m ON f.MEDAL_SK = m.MEDAL_SK
                WHERE UPPER(a.ATHLETE_NAME) LIKE UPPER('%{search}%')
                ORDER BY h.YEAR DESC
            """)
            st.dataframe(events_df, use_container_width=True)

else:
    st.info("Type an athlete name above to search")

    # ── Top Athletes ──
    st.subheader("🏆 Top 10 Athletes by Total Medals")
    top_athletes = run_query("""
        SELECT
            a.ATHLETE_NAME,
            p.TOTAL_MEDALS_WON      AS TOTAL_MEDALS,
            p.GOLD_MEDALS,
            p.SILVER_MEDALS,
            p.BRONZE_MEDALS,
            p.TOTAL_OLYMPICS_ATTENDED
        FROM OLYMPICS_DB.ANALYTICS.DIM_ATHLETE a
        JOIN OLYMPICS_DB.ANALYTICS.FACT_ATHLETE_PERFORMANCE p
            ON a.ATHLETE_SK = p.ATHLETE_SK
        ORDER BY TOTAL_MEDALS DESC
        LIMIT 10
    """)
    st.dataframe(top_athletes, use_container_width=True)