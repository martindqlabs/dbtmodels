{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

/*
  Records a new row only when the row count of DQLABS_QA.ZTEST.CLAIM
  (via ref('stg_claims')) differs from the last recorded snapshot.
  net_row_change = current_row_count - previous_row_count (net adds/removes).
*/

with current_volume as (
    select count(*) as row_count
    from {{ ref('stg_claims') }}
),

previous_volume as (
    {% if is_incremental() %}
    select row_count as prior_row_count
    from {{ this }}
    qualify row_number() over (order by snapshot_at desc) = 1
    {% else %}
    select null::number as prior_row_count
    {% endif %}
)

select
    cv.row_count as current_row_count,
    pv.prior_row_count as previous_row_count,
    cv.row_count - pv.prior_row_count as net_row_change,
    abs(cv.row_count - pv.prior_row_count) as abs_row_movement,
    current_timestamp() as snapshot_at,
    '{{ invocation_id }}' as dbt_invocation_id
from current_volume cv
left join previous_volume pv on 1 = 1
where
    {% if is_incremental() %}
    pv.prior_row_count is null
    or cv.row_count != pv.prior_row_count
    {% else %}
    true
    {% endif %}
