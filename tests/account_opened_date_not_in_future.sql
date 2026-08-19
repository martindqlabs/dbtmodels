select
    account_id,
    opened_date
from {{ ref('stg_accounts_customers') }}
where opened_date > current_date