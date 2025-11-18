{{ config(
    materialized='table'
) }}

select
    loan_id,
    account_id,
    amount,
    payments,
    purpose,
    status
from {{ source('dqlabs', 'bankloan') }}