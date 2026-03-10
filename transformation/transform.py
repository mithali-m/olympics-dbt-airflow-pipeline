import snowflake.connector
from dotenv import load_dotenv
from pathlib import Path
import os

load_dotenv(Path(__file__).parent.parent / "config" / ".env")


def get_snowflake_connection():
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )
    return conn


def create_schemas(cursor):
    print("Creating schemas...")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS OLYMPICS_DB.STAGING")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS OLYMPICS_DB.ANALYTICS")
    print("Schemas created")


def create_staging_table(cursor):
    print("Creating staging table...")
    cursor.execute("""
        CREATE OR REPLACE TABLE OLYMPICS_DB.STAGING.STG_ATHLETE_EVENTS AS
        SELECT
            ATHLETE_ID,
            TRIM(ATHLETE_NAME)                               AS ATHLETE_NAME,
            TRIM(GENDER)                                     AS GENDER,
            TRY_CAST(AGE AS INTEGER)                         AS AGE,
            TRY_CAST(DATE_OF_BIRTH AS DATE)                  AS DATE_OF_BIRTH,
            UPPER(TRIM(NATIONALITY))                         AS NATIONALITY,
            TRIM(COUNTRY_NAME)                               AS COUNTRY_NAME,
            TRIM(SPORT)                                      AS SPORT,
            TRIM(EVENT)                                      AS EVENT,
            TRIM(GAMES_TYPE)                                 AS GAMES_TYPE,
            TRY_CAST(YEAR AS INTEGER)                        AS YEAR,
            TRIM(HOST_CITY)                                  AS HOST_CITY,
            TRIM(TEAM_OR_INDIVIDUAL)                         AS TEAM_OR_INDIVIDUAL,
            CASE
                WHEN UPPER(TRIM(MEDAL)) = 'GOLD'   THEN 'Gold'
                WHEN UPPER(TRIM(MEDAL)) = 'SILVER' THEN 'Silver'
                WHEN UPPER(TRIM(MEDAL)) = 'BRONZE' THEN 'Bronze'
                ELSE 'No Medal'
            END                                              AS MEDAL,
            TRY_CAST(RESULT_VALUE AS FLOAT)                  AS RESULT_VALUE,
            TRIM(RESULT_UNIT)                                AS RESULT_UNIT,
            TRY_CAST(TOTAL_OLYMPICS_ATTENDED AS INTEGER)     AS TOTAL_OLYMPICS_ATTENDED,
            TRY_CAST(TOTAL_MEDALS_WON AS INTEGER)            AS TOTAL_MEDALS_WON,
            TRY_CAST(GOLD_MEDALS AS INTEGER)                 AS GOLD_MEDALS,
            TRY_CAST(SILVER_MEDALS AS INTEGER)               AS SILVER_MEDALS,
            TRY_CAST(BRONZE_MEDALS AS INTEGER)               AS BRONZE_MEDALS,
            TRY_CAST(COUNTRY_TOTAL_GOLD AS INTEGER)          AS COUNTRY_TOTAL_GOLD,
            TRY_CAST(COUNTRY_TOTAL_MEDALS AS INTEGER)        AS COUNTRY_TOTAL_MEDALS,
            TRY_CAST(COUNTRY_FIRST_PARTICIPATION AS INTEGER) AS COUNTRY_FIRST_PARTICIPATION,
            TRY_CAST(COUNTRY_BEST_RANK AS INTEGER)           AS COUNTRY_BEST_RANK,
            NULLIF(TRIM(IS_RECORD_HOLDER), '-')              AS IS_RECORD_HOLDER,
            NULLIF(TRIM(COACH_NAME), '-')                    AS COACH_NAME,
            TRY_CAST(HEIGHT_CM AS FLOAT)                     AS HEIGHT_CM,
            TRY_CAST(WEIGHT_KG AS FLOAT)                     AS WEIGHT_KG,
            CASE
                WHEN TRIM(NOTES) = '-' THEN 'No Notes'
                ELSE TRIM(NOTES)
            END                                              AS NOTES,
            LOADED_AT
        FROM OLYMPICS_DB.RAW.RAW_ATHLETE_EVENTS
        WHERE ATHLETE_ID IS NOT NULL
    """)
    print("Staging table created")


def create_dim_athlete(cursor):
    print("Creating dim_athlete...")
    cursor.execute("""
        CREATE OR REPLACE TABLE OLYMPICS_DB.ANALYTICS.DIM_ATHLETE AS
        SELECT DISTINCT
            MD5(ATHLETE_ID)         AS ATHLETE_SK,
            ATHLETE_ID,
            ATHLETE_NAME,
            GENDER,
            DATE_OF_BIRTH,
            IS_RECORD_HOLDER
        FROM OLYMPICS_DB.STAGING.STG_ATHLETE_EVENTS
        WHERE ATHLETE_ID IS NOT NULL
    """)
    print("dim_athlete created")


def create_dim_country(cursor):
    print("Creating dim_country...")
    cursor.execute("""
        CREATE OR REPLACE TABLE OLYMPICS_DB.ANALYTICS.DIM_COUNTRY AS
        SELECT DISTINCT
            MD5(NATIONALITY)                    AS COUNTRY_SK,
            COUNTRY_NAME,
            NATIONALITY                         AS COUNTRY_CODE,
            COUNTRY_FIRST_PARTICIPATION
        FROM OLYMPICS_DB.STAGING.STG_ATHLETE_EVENTS
        WHERE NATIONALITY IS NOT NULL
    """)
    print("dim_country created")


def create_dim_coach(cursor):
    print("Creating dim_coach...")
    cursor.execute("""
        CREATE OR REPLACE TABLE OLYMPICS_DB.ANALYTICS.DIM_COACH AS
        SELECT DISTINCT
            MD5(COALESCE(COACH_NAME, 'Unknown')) AS COACH_SK,
            COALESCE(COACH_NAME, 'Unknown')      AS COACH_NAME
        FROM OLYMPICS_DB.STAGING.STG_ATHLETE_EVENTS
    """)
    print("dim_coach created")


def create_dim_host(cursor):
    print("Creating dim_host...")
    cursor.execute("""
        CREATE OR REPLACE TABLE OLYMPICS_DB.ANALYTICS.DIM_HOST AS
        SELECT DISTINCT
            MD5(CONCAT(YEAR::VARCHAR, '|', HOST_CITY)) AS HOST_SK,
            YEAR,
            HOST_CITY
        FROM OLYMPICS_DB.STAGING.STG_ATHLETE_EVENTS
        WHERE YEAR IS NOT NULL
          AND HOST_CITY IS NOT NULL
    """)
    print("dim_host created")


def create_dim_sport(cursor):
    print("Creating dim_sport...")
    cursor.execute("""
        CREATE OR REPLACE TABLE OLYMPICS_DB.ANALYTICS.DIM_SPORT AS
        SELECT DISTINCT
            MD5(SPORT)  AS SPORT_SK,
            SPORT,
            GAMES_TYPE,
            RESULT_UNIT
        FROM OLYMPICS_DB.STAGING.STG_ATHLETE_EVENTS
        WHERE SPORT IS NOT NULL
    """)
    print("dim_sport created")


def create_dim_medals(cursor):
    print("Creating dim_medals...")
    cursor.execute("""
        CREATE OR REPLACE TABLE OLYMPICS_DB.ANALYTICS.DIM_MEDALS AS
        SELECT DISTINCT
            MD5(MEDAL)  AS MEDAL_SK,
            MEDAL
        FROM OLYMPICS_DB.STAGING.STG_ATHLETE_EVENTS
        WHERE MEDAL IS NOT NULL
    """)
    print("dim_medals created")


def create_dim_event(cursor):
    print("Creating dim_event...")
    cursor.execute("""
        CREATE OR REPLACE TABLE OLYMPICS_DB.ANALYTICS.DIM_EVENT AS
        SELECT DISTINCT
            MD5(CONCAT(EVENT, '|', TEAM_OR_INDIVIDUAL))  AS EVENT_SK,
            EVENT,
            TEAM_OR_INDIVIDUAL
        FROM OLYMPICS_DB.STAGING.STG_ATHLETE_EVENTS
        WHERE EVENT IS NOT NULL
    """)
    print("dim_event created")



def create_fact_athlete_performance(cursor):
    print("Creating fact_athlete_performance...")
    cursor.execute("""
        CREATE OR REPLACE TABLE OLYMPICS_DB.ANALYTICS.FACT_ATHLETE_PERFORMANCE AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY a.ATHLETE_SK)   AS ATHLETE_PERFORMANCE_SK,
            a.ATHLETE_SK,
            s.AGE,
            s.HEIGHT_CM,
            s.WEIGHT_KG,
            s.TOTAL_OLYMPICS_ATTENDED,
            s.TOTAL_MEDALS_WON,
            s.GOLD_MEDALS,
            s.SILVER_MEDALS,
            s.BRONZE_MEDALS,
            c.COACH_SK
        FROM OLYMPICS_DB.STAGING.STG_ATHLETE_EVENTS s
        LEFT JOIN OLYMPICS_DB.ANALYTICS.DIM_ATHLETE a
            ON s.ATHLETE_ID = a.ATHLETE_ID
        LEFT JOIN OLYMPICS_DB.ANALYTICS.DIM_COACH c
            ON COALESCE(s.COACH_NAME, 'Unknown') = c.COACH_NAME
    """)
    print("fact_athlete_performance created")


def create_fact_country_performance(cursor):
    print("Creating fact_country_performance...")
    cursor.execute("""
        CREATE OR REPLACE TABLE OLYMPICS_DB.ANALYTICS.FACT_COUNTRY_PERFORMANCE AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY co.COUNTRY_SK)  AS COUNTRY_PERFORMANCE_SK,
            co.COUNTRY_SK,
            s.COUNTRY_TOTAL_GOLD,
            s.COUNTRY_TOTAL_MEDALS,
            s.COUNTRY_FIRST_PARTICIPATION,
            s.COUNTRY_BEST_RANK
        FROM (
            SELECT DISTINCT
                NATIONALITY,
                COUNTRY_TOTAL_GOLD,
                COUNTRY_TOTAL_MEDALS,
                COUNTRY_FIRST_PARTICIPATION,
                COUNTRY_BEST_RANK
            FROM OLYMPICS_DB.STAGING.STG_ATHLETE_EVENTS
            WHERE NATIONALITY IS NOT NULL
        ) s
        LEFT JOIN OLYMPICS_DB.ANALYTICS.DIM_COUNTRY co
            ON s.NATIONALITY = co.COUNTRY_CODE
    """)
    print("fact_country_performance created")


def create_fact_event_performance(cursor):
    print("Creating fact_event_performance...")
    cursor.execute("""
        CREATE OR REPLACE TABLE OLYMPICS_DB.ANALYTICS.FACT_EVENT_PERFORMANCE AS
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY a.ATHLETE_SK, e.EVENT_SK, h.HOST_SK
            )                               AS EVENT_PERFORMANCE_SK,
            a.ATHLETE_SK,
            co.COUNTRY_SK,
            h.HOST_SK,
            sp.SPORT_SK,
            e.EVENT_SK,
            s.RESULT_VALUE,
            m.MEDAL_SK
        FROM OLYMPICS_DB.STAGING.STG_ATHLETE_EVENTS s
        LEFT JOIN OLYMPICS_DB.ANALYTICS.DIM_ATHLETE a
            ON s.ATHLETE_ID = a.ATHLETE_ID
        LEFT JOIN OLYMPICS_DB.ANALYTICS.DIM_COUNTRY co
            ON s.NATIONALITY = co.COUNTRY_CODE
        LEFT JOIN OLYMPICS_DB.ANALYTICS.DIM_HOST h
            ON s.YEAR = h.YEAR
            AND s.HOST_CITY = h.HOST_CITY
        LEFT JOIN OLYMPICS_DB.ANALYTICS.DIM_SPORT sp
            ON s.SPORT = sp.SPORT
            AND s.GAMES_TYPE = sp.GAMES_TYPE
        LEFT JOIN OLYMPICS_DB.ANALYTICS.DIM_EVENT e
            ON s.EVENT = e.EVENT
            AND s.TEAM_OR_INDIVIDUAL = e.TEAM_OR_INDIVIDUAL
        LEFT JOIN OLYMPICS_DB.ANALYTICS.DIM_MEDALS m
            ON s.MEDAL = m.MEDAL
    """)
    print("fact_event_performance created")



def run():
    print("Connecting to Snowflake...")
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    print("✓ Connected!")

    try:
        create_schemas(cursor)
        create_staging_table(cursor)

        # Dimensions first
        create_dim_athlete(cursor)
        create_dim_country(cursor)
        create_dim_coach(cursor)
        create_dim_host(cursor)
        create_dim_sport(cursor)
        create_dim_medals(cursor)
        create_dim_event(cursor)

        # Facts after (they depend on dimensions)
        create_fact_athlete_performance(cursor)
        create_fact_country_performance(cursor)
        create_fact_event_performance(cursor)

        conn.commit()
        print("\nTransformation complete! All tables are ready in Snowflake.")

    except Exception as e:
        print(f"\nError: {e}")
        raise

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    run()