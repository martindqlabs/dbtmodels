{{ config(
    materialized='view'
) }}

WITH ops_contacts AS (

    SELECT
        CONTACT_ID            AS ops_contact_id,
        ACCOUNT_ID            AS ops_account_id,
        CONTACT_EMAIL_ADDRESS AS ops_email,
        CONTACT_PHONE_NUMBER  AS ops_phone,
        ROLE                  AS ops_role,
        _BUILT_AT             AS ops_built_at

    FROM {{ ref('stg_crm_contacts') }}

),

procurement_contacts AS (

    SELECT
        CONTACT_ID            AS proc_contact_id,
        ACCOUNT_ID            AS proc_account_id,
        CONTACT_EMAIL_ADDRESS AS proc_email,
        CONTACT_PHONE_NUMBER  AS proc_phone,
        ROLE                  AS proc_role,
        _BUILT_AT             AS proc_built_at

    FROM {{ ref('SILVER_STG_CONTACTS') }}

)

SELECT * FROM ops_contacts
UNION ALL
SELECT * FROM procurement_contacts