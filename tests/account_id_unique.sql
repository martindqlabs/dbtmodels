select
    account_id,
    count(*) as row_count
from {{ ref('stg_accounts_customers') }}
group by account_id
having count(*) > 1