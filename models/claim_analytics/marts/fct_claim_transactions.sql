{{ config(
    materialized='incremental',
    unique_key=['CLAIM_ID', 'TX_ID'],
    incremental_strategy='merge',
    post_hook=[
        "delete from {{ this }} t where not exists (select 1 from {{ ref('stg_claim') }} s where s.CLAIM_ID = t.CLAIM_ID)"
    ]
) }}

with latest_claims as (
    select *
    from {{ ref('stg_claim') }}
    {% if is_incremental() %}
        where CLAIM_ID in (
            select src.CLAIM_ID
            from {{ ref('stg_claim') }} src
            left join (
                select
                    CLAIM_ID,
                    max(claim_updated_date) as target_updated_date
                from {{ this }}
                group by CLAIM_ID
            ) tgt
                on src.CLAIM_ID = tgt.CLAIM_ID
            where tgt.CLAIM_ID is null
               or coalesce(src.UPDATED_DATE, to_timestamp_ntz('1900-01-01'))
                > coalesce(tgt.target_updated_date, to_timestamp_ntz('1900-01-01'))
        )
    {% endif %}
    qualify row_number() over (
        partition by CLAIM_ID
        order by UPDATED_DATE desc
    ) = 1
),

claim_tx as (
    select *
    from {{ ref('stg_claim_tx') }}
)

select
    c.CLAIM_ID,
    tx.TX_ID,
    c.CLAIM_NUMBER,
    c.POLICY_NUMBER,
    c.CLAIM_TYPE,
    c.STATUS,
    c.STATE,
    c.CLAIM_DATE,
    c.ACCIDENT_DATE,
    c.REPORTED_DATE,
    c.UPDATED_DATE as claim_updated_date,
    tx.TX_DATE,
    tx.TX_AMOUNT,
    tx.TX_TYPE,
    tx.CREATED_DATE as tx_created_date,
    current_timestamp() as load_ts
from latest_claims c
left join claim_tx tx
    on c.CLAIM_ID = tx.CLAIM_ID
