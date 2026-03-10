import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
import os

from pathlib import Path
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


def setup_database(cursor):
    print("Setting up Snowflake database...")

    cursor.execute("CREATE DATABASE IF NOT EXISTS OLYMPICS_DB")
    cursor.execute("USE DATABASE OLYMPICS_DB")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS RAW")
    cursor.execute("USE SCHEMA RAW")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS RAW_ATHLETE_EVENTS (
            ATHLETE_ID                  VARCHAR(500),
            ATHLETE_NAME                VARCHAR(500),
            GENDER                      VARCHAR(500),
            AGE                         VARCHAR(500),
            DATE_OF_BIRTH               VARCHAR(500),
            NATIONALITY                 VARCHAR(500),
            COUNTRY_NAME                VARCHAR(500),
            SPORT                       VARCHAR(500),
            EVENT                       VARCHAR(500),
            GAMES_TYPE                  VARCHAR(500),
            YEAR                        VARCHAR(500),
            HOST_CITY                   VARCHAR(500),
            TEAM_OR_INDIVIDUAL          VARCHAR(500),
            MEDAL                       VARCHAR(500),
            RESULT_VALUE                VARCHAR(500),
            RESULT_UNIT                 VARCHAR(500),
            TOTAL_OLYMPICS_ATTENDED     VARCHAR(500),
            TOTAL_MEDALS_WON            VARCHAR(500),
            GOLD_MEDALS                 VARCHAR(500),
            SILVER_MEDALS               VARCHAR(500),
            BRONZE_MEDALS               VARCHAR(500),
            COUNTRY_TOTAL_GOLD          VARCHAR(500),
            COUNTRY_TOTAL_MEDALS        VARCHAR(500),
            COUNTRY_FIRST_PARTICIPATION VARCHAR(500),
            COUNTRY_BEST_RANK           VARCHAR(500),
            IS_RECORD_HOLDER            VARCHAR(500),
            COACH_NAME                  VARCHAR(500),
            HEIGHT_CM                   VARCHAR(500),
            WEIGHT_KG                   VARCHAR(500),
            NOTES                       VARCHAR(500),
            LOADED_AT                   TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    print("Table created (or already exists)")


def load_csv_to_snowflake(csv_path: str):
    print(f"Reading CSV from: {csv_path}")
    df = pd.read_csv(csv_path)
    print(f"CSV loaded: {len(df):,} rows, {len(df.columns)} columns")
    return df


def insert_data(cursor, df: pd.DataFrame):
    print("Inserting data into Snowflake...")

    # Replace NaN with None so Snowflake stores them as NULL
    df = df.where(pd.notna(df), None)

    # Convert all rows to a list of tuples
    rows = [
        (
            row.get("athlete_id"), row.get("athlete_name"),
            row.get("gender"), row.get("age"),
            row.get("date_of_birth"), row.get("nationality"),
            row.get("country_name"), row.get("sport"),
            row.get("event"), row.get("games_type"),
            row.get("year"), row.get("host_city"),
            row.get("team_or_individual"), row.get("medal"),
            row.get("result_value"), row.get("result_unit"),
            row.get("total_olympics_attended"), row.get("total_medals_won"),
            row.get("gold_medals"), row.get("silver_medals"),
            row.get("bronze_medals"), row.get("country_total_gold"),
            row.get("country_total_medals"), row.get("country_first_participation"),
            row.get("country_best_rank"), row.get("is_record_holder"),
            row.get("coach_name"), row.get("height_cm"),
            row.get("weight_kg"), row.get("notes")
        )
        for _, row in df.iterrows()
    ]

    # Insert all rows in one batch
    cursor.executemany("""
            INSERT INTO RAW_ATHLETE_EVENTS (
                ATHLETE_ID, ATHLETE_NAME, GENDER, AGE, DATE_OF_BIRTH,
                NATIONALITY, COUNTRY_NAME, SPORT, EVENT, GAMES_TYPE,
                YEAR, HOST_CITY, TEAM_OR_INDIVIDUAL, MEDAL, RESULT_VALUE,
                RESULT_UNIT, TOTAL_OLYMPICS_ATTENDED, TOTAL_MEDALS_WON,
                GOLD_MEDALS, SILVER_MEDALS, BRONZE_MEDALS,
                COUNTRY_TOTAL_GOLD, COUNTRY_TOTAL_MEDALS,
                COUNTRY_FIRST_PARTICIPATION, COUNTRY_BEST_RANK,
                IS_RECORD_HOLDER, COACH_NAME, HEIGHT_CM, WEIGHT_KG, NOTES
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """, rows)

    print(f"Done! {len(rows):,} rows inserted into Snowflake")


def run():
    csv_path = Path(__file__).parent.parent / "data" / "olympics_athletes_dataset.csv"

    print("Connecting to Snowflake...")
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    print("Connected!")

    try:
        setup_database(cursor)
        df = load_csv_to_snowflake(csv_path)
        insert_data(cursor, df)
        conn.commit()
        print("\nIngestion complete!")

    except Exception as e:
        print(f"\nError: {e}")
        raise

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    run()