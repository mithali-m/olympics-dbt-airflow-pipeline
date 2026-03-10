WITH stg AS (
    SELECT * FROM {{ ref('stg_athlete_events') }}
),

dim_athlete AS (
    SELECT * FROM {{ ref('dim_athlete') }}
),

dim_country AS (
    SELECT * FROM {{ ref('dim_country') }}
),

dim_host AS (
    SELECT * FROM {{ ref('dim_host') }}
),

dim_sport AS (
    SELECT * FROM {{ ref('dim_sport') }}
),

dim_event AS (
    SELECT * FROM {{ ref('dim_event') }}
),

dim_medals AS (
    SELECT * FROM {{ ref('dim_medals') }}
)

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
FROM stg s
LEFT JOIN dim_athlete a
    ON s.ATHLETE_ID = a.ATHLETE_ID
LEFT JOIN dim_country co
    ON s.NATIONALITY = co.COUNTRY_CODE
LEFT JOIN dim_host h
    ON s.YEAR = h.YEAR
    AND s.HOST_CITY = h.HOST_CITY
LEFT JOIN dim_sport sp
    ON s.SPORT = sp.SPORT
    AND s.GAMES_TYPE = sp.GAMES_TYPE
LEFT JOIN dim_event e
    ON s.EVENT = e.EVENT
    AND s.TEAM_OR_INDIVIDUAL = e.TEAM_OR_INDIVIDUAL
LEFT JOIN dim_medals m
    ON s.MEDAL = m.MEDAL