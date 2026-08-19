{{ config(
    materialized = 'table'
) }}

select
    CLAIM_ID,
    CLAIM_NUMBER,
    POLICY_NUMBER,
    CLAIM_DATE,
    CLAIM_TYPE,
    STATE,
    STATUS,
    ACCIDENT_DATE,
    CREATED_DATE,
    REPORTED_DATE,
    UPDATED_DATE
from DQLABS_QA.ZTEST.CLAIM
