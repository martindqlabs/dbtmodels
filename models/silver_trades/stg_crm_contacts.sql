{{ config(
    materialized='table'
) }}

WITH source_data AS (

    SELECT
        ACCOUNT_ID,
        CONTACT_EMAIL_ADDRESS,
        CONTACT_ID,
        CONTACT_PHONE_NUMBER,
        ROLE,
        _BUILT_AT

    FROM {{ source('PRIZM_BANKING', 'STG_CRM_CONTACTS_NEW') }} 
    WHERE ROLE = 'ops' 

)

SELECT
    ACCOUNT_ID,
    CONTACT_EMAIL_ADDRESS,
    CONTACT_ID,
    CONTACT_PHONE_NUMBER,
    ROLE,
    _BUILT_AT

FROM source_data 