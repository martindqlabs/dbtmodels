{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

/*
  One append per dbt run: compares fct_claimsdata row counts to the prior snapshot row
  (latest snapshot_at in this table). Use net_row_change as “rows volume delta” since
  the last run; 0 means no change. For Snowflake MERGE rows_affected, see dbt run_results.
*/

with current_volume as (
    select
        count(*) as row_count,
        count(distinct claim_id) as distinct_claim_count
    from {{ ref('fct_claimsdata') }}
),

previous_volume as (
    {% if is_incremental() %}
    select
        row_count as prior_row_count,
        distinct_claim_count as prior_distinct_claim_count
    from {{ this }}
    qualify row_number() over (order by snapshot_at desc) = 1
    {% else %}
    select
        null::number as prior_row_count,
        null::number as prior_distinct_claim_count
    {% endif %}
)

select
    cv.row_count as current_row_count,
    cv.distinct_claim_count as current_distinct_claim_count,
    pv.prior_row_count as previous_row_count,
    pv.prior_distinct_claim_count as previous_distinct_claim_count,
    cv.row_count - pv.prior_row_count as net_row_change,
    cv.distinct_claim_count - pv.prior_distinct_claim_count as net_distinct_claim_change,
    abs(cv.row_count - pv.prior_row_count) as abs_row_movement,
    (
        pv.prior_row_count is null
        or cv.row_count != pv.prior_row_count
        or coalesce(cv.distinct_claim_count, -1) != coalesce(pv.prior_distinct_claim_count, -1)
    ) as volume_changed,
    current_timestamp() as snapshot_at,
    '{{ invocation_id }}' as dbt_invocation_id
from current_volume cv
left join previous_volume pv on 1 = 1
