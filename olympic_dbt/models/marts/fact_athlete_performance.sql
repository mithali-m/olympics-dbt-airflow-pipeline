WITH stg AS (
    SELECT * FROM {{ ref('stg_athlete_events') }}
),

dim_athlete AS (
    SELECT * FROM {{ ref('dim_athlete') }}
),

dim_coach AS (
    SELECT * FROM {{ ref('dim_coach') }}
)

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
FROM stg s
LEFT JOIN dim_athlete a
    ON s.ATHLETE_ID = a.ATHLETE_ID
LEFT JOIN dim_coach c
    ON COALESCE(s.COACH_NAME, 'No Coach') = c.COACH_NAME