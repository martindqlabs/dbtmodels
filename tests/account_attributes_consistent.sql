select
    account_id
from {{ ref('stg_accounts_customers') }}
group by account_id
having count(distinct account_type) > 1
    or count(distinct branch_id) > 1
    or count(distinct account_status) > 1
    or count(distinct opened_date) > 1