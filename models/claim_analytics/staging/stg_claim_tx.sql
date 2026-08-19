{{ config(
    materialized='incremental',
    unique_key=['CLAIM_ID', 'TX_ID'],
    incremental_strategy='merge',
) }}

{# MERGE requires at most one source row per unique_key; CLAIM_TX can repeat keys. #}
with src as (
    select TX_ID, CLAIM_ID, TX_DATE, TX_AMOUNT, TX_TYPE, TX_CATEGORY, CREATED_DATE
    from {{ source('ztest', 'CLAIM_TX') }}
    {# CLAIM_TX has no UPDATED_DATE; use row / line timestamps from the source. #}
    {% if is_incremental() %}
        where coalesce(CREATED_DATE, TX_DATE, to_timestamp_ntz('1900-01-01'))
            >= (
                select coalesce(
                    max(coalesce(CREATED_DATE, TX_DATE)),
                    to_timestamp_ntz('1900-01-01')
                )
                from {{ this }}
            )
    {% endif %}
)

select *
from src
qualify row_number() over (
    partition by CLAIM_ID, TX_ID
    order by coalesce(CREATED_DATE, TX_DATE) desc nulls last
) = 1
