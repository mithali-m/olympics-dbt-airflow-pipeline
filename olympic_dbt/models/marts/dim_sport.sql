WITH stg AS (
    SELECT * FROM {{ ref('stg_athlete_events') }}
)

SELECT DISTINCT
            MD5(SPORT)  AS SPORT_SK,
            SPORT,
            GAMES_TYPE,
            RESULT_UNIT
FROM stg
WHERE SPORT IS NOT NULL