{{ config(materialized='table') }}
select
    customer_id,
    first_name,
    last_name
from `bionic-genre-363105.SADBT.customers1`
