select *
from {{ ref('stg_claims') }}
where AMOUNT < 0
