WITH stg AS (
    SELECT * FROM {{ ref('stg_athlete_events') }}
)

SELECT DISTINCT
    MD5(MEDAL)  AS MEDAL_SK,
    MEDAL
FROM stg
WHERE MEDAL IS NOT NULL