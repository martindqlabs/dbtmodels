{{ config(materialized='table') }}
select
    order_id,
    customer_id,
    order_date,
    status
from `bionic-genre-363105`.SADBT.orders1