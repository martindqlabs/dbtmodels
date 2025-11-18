{{ config(materialized='view') }}

with client as (
    select *
    from {{ ref('stg_bankclient') }}
),

loan as (
    select *
    from {{ ref('stg_bankloan') }}
)

select
    loan.loan_id,
    loan.account_id,
    loan.amount,
    loan.payments,
    loan.purpose,
    loan.status,
    client.client_id,
    client.first_name,
    client.last_name,
    client.email,
    client.phone_number
from loan
left join client
    on loan.account_id = client.account_id
