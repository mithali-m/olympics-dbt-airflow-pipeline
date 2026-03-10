WITH source AS (
    SELECT * FROM OLYMPICS_DB.RAW.RAW_ATHLETE_EVENTS
),

cleaned AS (
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
            COALESCE(TRIM(IS_RECORD_HOLDER), 'No')           AS IS_RECORD_HOLDER,
            COALESCE(TRIM(COACH_NAME), 'No Coach')           AS COACH_NAME,
            TRY_CAST(HEIGHT_CM AS FLOAT)                     AS HEIGHT_CM,
            TRY_CAST(WEIGHT_KG AS FLOAT)                     AS WEIGHT_KG,
            CASE
                WHEN TRIM(NOTES) = '-' THEN 'No Notes'
                ELSE TRIM(NOTES)
            END                                              AS NOTES,
            LOADED_AT
        FROM OLYMPICS_DB.RAW.RAW_ATHLETE_EVENTS
        WHERE ATHLETE_ID IS NOT NULL
)

SELECT * FROM cleaned