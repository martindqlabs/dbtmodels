{{ config(materialized='view') }}

select
    txn_id,
    account_id,
    txn_date,
    amount                     as txn_amount,
    txn_type,
    created_at
from {{ source('banking', 'transactions_dbt') }}