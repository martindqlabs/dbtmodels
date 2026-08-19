{{ config(materialized="view", tags=["mart"]) }}

select
    claim_id,
    claim_number,
    policy_number,
    state,
    status,

    count(tx_id) as total_transactions,
    sum(tx_amount) as total_claim_amount,

    min(claim_date) as claim_date,
    max(tx_date) as last_transaction_date

from {{ ref("fct_claimsdata") }}
group by claim_id, claim_number, policy_number, state, status
