{{ config(
    materialized='view',
    schema='report'
) }}

select
    a.txn_date,
    a.account_id,
    d.account_type,
    d.branch_id,
    d.customer_id,
    d.customer_name,
    d.risk_rating,
    d.customer_segment,
    d.country,
    a.suspicious_amount,
    a.txn_amount,
    a.txn_count,
    a.aml_load_ts
from {{ ref('stg_daily_account_activity') }} a
left join {{ ref('stg_accounts_customers') }} d
    on a.account_id = d.account_id
where a.suspicious_amount is not null
  and a.suspicious_amount > 0