{{ config(
    materialized='table',
    schema='report'
) }}

with txn_daily_rollup as (
    select
        txn_date,
        account_id,
        sum(txn_amount)               as computed_txn_amount,
        count(txn_id)                 as computed_txn_count
    from {{ ref('stg_banking_transactions') }}
    group by 1, 2
),

activity as (
    select * from {{ ref('stg_daily_account_activity') }}
)

select
    a.txn_date,
    a.account_id,
    a.txn_amount,
    a.txn_count,
    a.suspicious_amount,
    t.computed_txn_amount,
    t.computed_txn_count,
    a.load_ts,
    current_timestamp()               as report_generated_at
from activity a
left join txn_daily_rollup t
    on a.txn_date = t.txn_date
    and a.account_id = t.account_id