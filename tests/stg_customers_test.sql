{{ config(store_failures = true) }}
select
    *
from {{ ref('stg_customer') }}
where first_name = 'Mildred'