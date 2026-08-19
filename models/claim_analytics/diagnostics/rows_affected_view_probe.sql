{{ config(
    materialized='view',
    tags=['claim_analytics', 'observability', 'rows_affected_probe']
) }}

/*
  Rows-affected probe (VIEW): adapters often report rows_affected = -1 for views — not an error.
  Confirms hooks/logs treat success + N/A row counts without false failure.

  Depends on existing claim_analytics models only.
*/
select
    f.claim_id,
    f.tx_id,
    f.claim_number,
    f.tx_amount,
    f.tx_type,
    c.status as claim_status,
    t.tx_created_date
from {{ ref('fct_claim_transactions') }} as f
left join {{ ref('stg_claim') }} as c
    on c.claim_id = f.claim_id
left join (
    select
        claim_id,
        tx_id,
        created_date as tx_created_date
    from {{ ref('stg_claim_tx') }}
) as t
    on t.claim_id = f.claim_id
    and t.tx_id = f.tx_id
