{{ config(materialized='view') }}

with source_data as (
    select
        FirstName,
        LastName,
        Title,
        Gen,
        Snumber,
        Salary,
        Loan_Type,
        CreditScore,
        Updated_Date
    from {{ source('synapse_dqlabs', 'Personal_Loan_Data') }}
),

renamed as (
    select
        FirstName as first_name,
        LastName as last_name,
        Title as title,
        Gen as gender,
        Snumber as ssn,
        Salary as salary,
        Loan_Type as loan_type,
        CreditScore as credit_score,
        Updated_Date as updated_date
    from source_data
)

select *
from renamed;
