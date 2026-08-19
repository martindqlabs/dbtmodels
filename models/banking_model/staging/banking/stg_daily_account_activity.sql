{{ config(materialized='view') }}

with daily as (

    select *
    from {{ source('banking', 'daily_transactions_dbt') }}
    order by txn_date desc

),

aml as (

    select *
    from {{ source('banking', 'aml_transactions_dbt') }}
    order by txn_date desc

),

combined as (

    select
        coalesce(d.txn_date, a.txn_date)     as txn_date,
        coalesce(d.account_id, a.account_id) as account_id,
        d.txn_amount,
        d.txn_count,
        a.suspicious_amount,
        d.load_ts,
        a.load_ts as aml_load_ts

    from daily d

    full outer join aml a
        on d.txn_date = a.txn_date
        and d.account_id = a.account_id

    where d.txn_amount < 10000

)

select *
from combined