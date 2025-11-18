{{ config(
    materialized = 'table'
) }}

WITH source_data AS (
    SELECT
        CLIENT_ID,
        FIRST,
        LAST,
        EMAIL,
        AGE,
        CITY,
        STATE,
        CAST(FULLDATE AS DATE) AS full_date
    FROM {{ source('dqlabs', 'bankclient') }}
),

cleaned AS (
    SELECT
        CLIENT_ID AS client_id,
        FIRST AS first_name,
        LAST AS last_name,
        EMAIL AS email,
        AGE AS age,
        CITY AS city,
        STATE AS state,
        full_date
    FROM source_data
)

SELECT *
FROM cleaned;
