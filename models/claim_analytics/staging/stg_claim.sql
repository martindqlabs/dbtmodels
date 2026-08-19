{{ config(
    materialized='incremental',
    unique_key='CLAIM_ID',
    incremental_strategy='merge',
) }}

{# MERGE allows one source row per unique_key; CLAIM can repeat CLAIM_ID. #}
with src as (
    select   CLAIM_ID,
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
    from {{ source('ztest', 'CLAIM') }}
    {% if is_incremental() %}
        where coalesce(UPDATED_DATE, to_timestamp_ntz('1900-01-01'))
            >= (
                select coalesce(max(UPDATED_DATE), to_timestamp_ntz('1900-01-01'))
                from {{ this }}
            )
    {% endif %}
)

select *
from src
qualify row_number() over (
    partition by CLAIM_ID
    order by coalesce(UPDATED_DATE, CREATED_DATE) desc nulls last
) = 1
