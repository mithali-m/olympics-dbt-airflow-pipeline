WITH stg AS (
    SELECT * FROM {{ ref('stg_athlete_events') }}
)

SELECT DISTINCT
            MD5(COALESCE(COACH_NAME, 'No Coach')) AS COACH_SK,
            COALESCE(COACH_NAME, 'No Coach')      AS COACH_NAME
FROM stg