select *
from {{ ref('stg_claims') }}
where POLICY_NUMBER is not null
  and try_cast(POLICY_NUMBER as number) is null
