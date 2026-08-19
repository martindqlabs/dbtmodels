{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

/*
  One row per dbt run: staging + fact row counts and distinct claim grain on the fact.
  Compare to the previous run in this table to see net change (ingest / merge health)
  without trusting run_results.rows_affected on Snowflake CTAS.

  - net_* = 0 → no net change vs last run (still useful for SLA “we ran” audit).
  - src_claim_rows_* → counts straight from ZTEST.CLAIM for “did source change Today”.
*/

with src_today as (
    select
        count(*) as src_claim_rows_total,
        count_if(
            coalesce(UPDATED_DATE, CREATED_DATE)::date = current_date()
        ) as src_claim_rows_updated_or_created_today
    from {{ source('ztest', 'CLAIM') }}
),

current_snapshot as (
    select
        (select count(*) from {{ ref('stg_claim') }}) as stg_claim_rows,
        (select count(*) from {{ ref('stg_claim_tx') }}) as stg_claim_tx_rows,
        (select count(*) from {{ ref('fct_claim_transactions') }}) as fct_rows,
        (
            select count(distinct claim_id)
            from {{ ref('fct_claim_transactions') }}
        ) as fct_distinct_claims
),

curr as (
    select
        cs.*,
        s.src_claim_rows_total,
        s.src_claim_rows_updated_or_created_today
    from current_snapshot cs
    cross join src_today s
),

previous as (
    {% if is_incremental() %}
        select
            stg_claim_rows as prev_stg_claim_rows,
            stg_claim_tx_rows as prev_stg_claim_tx_rows,
            fct_rows as prev_fct_rows,
            fct_distinct_claims as prev_fct_distinct_claims,
            src_claim_rows_total as prev_src_claim_rows_total
        from {{ this }}
        qualify row_number() over (order by snapshot_at desc) = 1
    {% else %}
        select
            null::number as prev_stg_claim_rows,
            null::number as prev_stg_claim_tx_rows,
            null::number as prev_fct_rows,
            null::number as prev_fct_distinct_claims,
            null::number as prev_src_claim_rows_total
    {% endif %}
)

select
    c.stg_claim_rows,
    c.stg_claim_tx_rows,
    c.fct_distinct_claims,
    c.src_claim_rows_total,
    c.src_claim_rows_updated_or_created_today,
    p.prev_stg_claim_rows,
    p.prev_stg_claim_tx_rows,
    p.prev_fct_rows,
    p.prev_fct_distinct_claims,
    p.prev_src_claim_rows_total,
    c.stg_claim_rows - p.prev_stg_claim_rows as net_stg_claim_rows,
    c.stg_claim_tx_rows - p.prev_stg_claim_tx_rows as net_stg_claim_tx_rows,
    c.fct_rows - p.prev_fct_rows as net_fct_rows,
    c.fct_distinct_claims - p.prev_fct_distinct_claims as net_distinct_claims,
    c.src_claim_rows_total - p.prev_src_claim_rows_total as net_src_claim_rows_total,
    current_timestamp() as snapshot_at,
    '{{ invocation_id }}'::varchar as dbt_invocation_id
from curr c
cross join previous p
