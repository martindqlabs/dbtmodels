{{ config(materialized='table') }}

with base as (
    select *
    from {{ ref('stg_personal_loan_data') }}
),

credit_bucket as (
    select
        *,
        case
            when credit_score >= '650' then 'Excellent'
            when credit_score >= '550' then 'Good'
            else 'Poor'
        end as credit_score_bucket
    from base
)

select *
from credit_bucket;
