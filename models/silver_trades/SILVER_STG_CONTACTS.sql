{{ config(
    materialized='table'
) }}

WITH source_data AS (

    SELECT
        CONTACT_ID,
        ACCOUNT_ID,
        CONTACT_EMAIL_ADDRESS,
        CONTACT_PHONE_NUMBER,
        ROLE,
        _BUILT_AT

    FROM {{ source('PRIZM_BANKING', 'STG_CRM_CONTACTS_NEW') }}
    WHERE ROLE = 'procurement'

)

SELECT
    CONTACT_ID,
    ACCOUNT_ID,
    CONTACT_EMAIL_ADDRESS,
    CONTACT_PHONE_NUMBER,
    ROLE,
    _BUILT_AT

FROM source_data