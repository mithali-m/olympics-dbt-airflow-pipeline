WITH stg AS (
    SELECT * FROM {{ ref('stg_athlete_events') }}
)

SELECT DISTINCT
            MD5(CONCAT(EVENT, '|', TEAM_OR_INDIVIDUAL))  AS EVENT_SK,
            EVENT,
            TEAM_OR_INDIVIDUAL
FROM stg
WHERE EVENT IS NOT NULL