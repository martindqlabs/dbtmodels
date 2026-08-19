{{ config(materialized='view') }}

with accounts as (
    select
        account_id,
        account_type,
        branch_id,
        currency,
        status, 
        opened_date
    from {{ source('banking', 'accounts') }}
),

customers as (
    select
        customer_id,
        account_id,
        customer_name,
        risk_rating,
        customer_segment,
        country
    from {{ source('banking', 'customers') }}
)

select
    a.account_id,
    a.account_type,
    a.branch_id,
    a.currency,
    a.status                as account_status,
    a.opened_date,
    c.customer_id,
    c.customer_name,
    c.risk_rating,
    c.customer_segment,
    c.country
from accounts a
left join customers c
    on a.account_id = c.account_id