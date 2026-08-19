{{ config(
    materialized='custom_incremental',
    unique_key=['CLAIM_ID','TX_ID']
) }}

-- Step 1: Source claims
with source_data as (
    select
        CLAIM_ID,
        CLAIM_NUMBER,
        POLICY_NUMBER,
        CLAIM_TYPE,
        STATUS,
        STATE,
        CLAIM_DATE,
        ACCIDENT_DATE,
        REPORTED_DATE,
        UPDATED_DATE
    from {{ ref('stg_claims') }}
),

-- Step 2: Deduplicate claim transactions
claim_tx as (
    select *
    from (
        select
            CLAIM_ID,
            TX_ID,
            TX_DATE,
            TX_AMOUNT,
            TX_TYPE,
            CREATED_DATE as TX_CREATED_DATE,
            row_number() over (
                partition by CLAIM_ID, TX_ID
                order by CREATED_DATE desc
            ) as rn
        from {{ ref('stg_claimstx') }}
    )
    where rn = 1
),

-- Step 3: Join claims and transactions
final as (
    select
        s.CLAIM_ID,
        tx.TX_ID,
        s.CLAIM_NUMBER,
        s.POLICY_NUMBER,
        s.CLAIM_TYPE,
        s.STATUS,
        s.STATE,
        s.CLAIM_DATE,
        s.ACCIDENT_DATE,
        s.REPORTED_DATE,
        tx.TX_DATE,
        s.UPDATED_DATE,
        tx.TX_CREATED_DATE,
        tx.TX_AMOUNT,
        tx.TX_TYPE,
        current_timestamp() as LOAD_TS
    from source_data s
    left join claim_tx tx
        on s.CLAIM_ID = tx.CLAIM_ID
)

-- Step 4: Apply incremental filter
select *
from final
{% if is_incremental() %}
where UPDATED_DATE > (
    select coalesce(max(UPDATED_DATE), '1900-01-01')
    from {{ this }}
)
{% endif %}