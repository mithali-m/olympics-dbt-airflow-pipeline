import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path
import os
from datetime import datetime, timezone

load_dotenv(Path(__file__).parent.parent / "config" / ".env")


def get_connection():
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema="MONITORING"
    )
    return conn


def setup_monitoring(cursor):
    """Create monitoring schema and tables"""
    print("Setting up monitoring...")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS OLYMPICS_DB.MONITORING")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS OLYMPICS_DB.MONITORING.PIPELINE_RUN_LOG (
            RUN_ID          VARCHAR(100),
            STEP_NAME       VARCHAR(100),
            STATUS          VARCHAR(20),
            ROWS_PROCESSED  INTEGER,
            STARTED_AT      TIMESTAMP,
            COMPLETED_AT    TIMESTAMP,
            DURATION_SECS   FLOAT,
            ERROR_MESSAGE   VARCHAR(5000)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS OLYMPICS_DB.MONITORING.TABLE_ROW_COUNTS (
            RECORDED_AT     TIMESTAMP,
            TABLE_NAME      VARCHAR(100),
            ROW_COUNT       INTEGER
        )
    """)
    print("✓ Monitoring tables created")


def log_run(cursor, run_id: str, step: str, status: str,
            rows: int, started_at: datetime, error: str = None):
    """Log a pipeline step result"""
    completed_at = datetime.now(timezone.utc)
    duration = (completed_at - started_at).total_seconds()

    cursor.execute("""
        INSERT INTO OLYMPICS_DB.MONITORING.PIPELINE_RUN_LOG
            (RUN_ID, STEP_NAME, STATUS, ROWS_PROCESSED,
             STARTED_AT, COMPLETED_AT, DURATION_SECS, ERROR_MESSAGE)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (run_id, step, status, rows, started_at, completed_at, duration, error))


def log_table_counts(cursor):
    """Record row counts for all tables"""
    print("Recording table row counts...")
    tables = [
        "OLYMPICS_DB.STAGING.STG_ATHLETE_EVENTS",
        "OLYMPICS_DB.ANALYTICS.DIM_ATHLETE",
        "OLYMPICS_DB.ANALYTICS.DIM_COUNTRY",
        "OLYMPICS_DB.ANALYTICS.DIM_COACH",
        "OLYMPICS_DB.ANALYTICS.DIM_HOST",
        "OLYMPICS_DB.ANALYTICS.DIM_SPORT",
        "OLYMPICS_DB.ANALYTICS.DIM_MEDALS",
        "OLYMPICS_DB.ANALYTICS.DIM_EVENT",
        "OLYMPICS_DB.ANALYTICS.FACT_ATHLETE_PERFORMANCE",
        "OLYMPICS_DB.ANALYTICS.FACT_COUNTRY_PERFORMANCE",
        "OLYMPICS_DB.ANALYTICS.FACT_EVENT_PERFORMANCE",
    ]

    recorded_at = datetime.now(timezone.utc)
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        cursor.execute("""
            INSERT INTO OLYMPICS_DB.MONITORING.TABLE_ROW_COUNTS
                (RECORDED_AT, TABLE_NAME, ROW_COUNT)
            VALUES (%s, %s, %s)
        """, (recorded_at, table, count))
        print(f"  {table.split('.')[-1]:<35} {count:>8,} rows")


def run_data_quality_checks(cursor) -> list:
    """Run basic data quality checks"""
    print("\nRunning data quality checks...")
    checks = [
        {
            "name": "No null athlete IDs in dim_athlete",
            "sql": "SELECT COUNT(*) FROM OLYMPICS_DB.ANALYTICS.DIM_ATHLETE WHERE ATHLETE_ID IS NULL",
            "expected": 0
        },
        {
            "name": "No null country codes in dim_country",
            "sql": "SELECT COUNT(*) FROM OLYMPICS_DB.ANALYTICS.DIM_COUNTRY WHERE COUNTRY_CODE IS NULL",
            "expected": 0
        },
        {
            "name": "Valid medal values only",
            "sql": """SELECT COUNT(*) FROM OLYMPICS_DB.ANALYTICS.DIM_MEDALS
                      WHERE MEDAL NOT IN ('Gold', 'Silver', 'Bronze', 'No Medal')""",
            "expected": 0
        },
        {
            "name": "Staging has data",
            "sql": "SELECT COUNT(*) FROM OLYMPICS_DB.STAGING.STG_ATHLETE_EVENTS",
            "expected_min": 1
        },
        {
            "name": "No orphaned athletes in fact",
            "sql": """SELECT COUNT(*) FROM OLYMPICS_DB.ANALYTICS.FACT_ATHLETE_PERFORMANCE
                      WHERE ATHLETE_SK IS NULL""",
            "expected": 0
        },
        {
            "name": "Fact event performance has data",
            "sql": "SELECT COUNT(*) FROM OLYMPICS_DB.ANALYTICS.FACT_EVENT_PERFORMANCE",
            "expected_min": 1
        },
    ]

    results = []
    for check in checks:
        cursor.execute(check["sql"])
        actual = cursor.fetchone()[0]

        if "expected_min" in check:
            passed = actual >= check["expected_min"]
        else:
            passed = actual == check["expected"]

        status = "PASS" if passed else "FAIL"
        print(f"  {status} | {check['name']} (got {actual})")
        results.append({
            "check": check["name"],
            "passed": passed,
            "actual": actual
        })

    return results


def print_pipeline_history(cursor):
    """Print recent pipeline run history"""
    print("\nRecent Pipeline Runs:")
    cursor.execute("""
        SELECT
            RUN_ID,
            STEP_NAME,
            STATUS,
            ROWS_PROCESSED,
            DURATION_SECS,
            STARTED_AT
        FROM OLYMPICS_DB.MONITORING.PIPELINE_RUN_LOG
        ORDER BY STARTED_AT DESC
        LIMIT 10
    """)
    rows = cursor.fetchall()
    if not rows:
        print("  No pipeline runs recorded yet")
    else:
        for row in rows:
            print(f"  [{row[5]}] {row[1]:<30} {row[2]:<10} {row[3]:>8,} rows  {row[4]:.1f}s")


def run():
    import uuid
    run_id = str(uuid.uuid4())[:8]
    started_at = datetime.now(timezone.utc)

    print(f"Olympic ETL Monitor — Run ID: {run_id}")
    print(f"   Started at: {started_at}")
    print("=" * 60)

    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Setup
        setup_monitoring(cursor)
        conn.commit()

        # Log table counts
        print("\nTable Row Counts:")
        log_table_counts(cursor)

        # Data quality checks
        results = run_data_quality_checks(cursor)
        passed = sum(1 for r in results if r["passed"])
        total = len(results)

        # Log this monitoring run
        status = "SUCCESS" if passed == total else "WARNING"
        log_run(cursor, run_id, "monitoring", status, 0, started_at)
        conn.commit()

        # Pipeline history
        print_pipeline_history(cursor)

        print("\n" + "=" * 60)
        print(f"Data Quality: {passed}/{total} checks passed")
        print(f"   Status: {status}")

    except Exception as e:
        print(f"\nError: {e}")
        raise

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    run()