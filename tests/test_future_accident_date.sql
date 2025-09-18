select *
from {{ ref('stg_claims') }}
where ACCIDENT_DATE > current_date
