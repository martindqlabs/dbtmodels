{{ config(materialized='table') }}

with loan_data as (
    select *
    from {{ ref('fct_personal_loan_customers') }}
),

country_data as (
    select *
    from {{ ref('stg_country_codes_sample') }}
),

joined as (
    select
        loan_data.*,
        country_data.country,
        country_data.iso_code,
        country_data.dialing_code
    from loan_data
    left join country_data
        on loan_data.gender = country_data.iso_code   -- MODIFY IF NEEDED
)

select *
from joined;
