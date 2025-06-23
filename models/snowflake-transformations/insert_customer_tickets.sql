{{
    config(
        materialized='incremental',
        unique_key='ticket_id'
    )
}}

SELECT 
    t.ticket_id,
    t.customer_id,
    t.issue_type,
    t.description,
    TRY_TO_TIMESTAMP(t.ticket_date) AS ticket_date,
    t.resolution_status,
    t.first_name,
    t.last_name,
    t.email,
    t.phone_number,
    t.join_date,
    t.status,
    t.loyalty_points
FROM {{ ref('customer_tickets') }} t

{% if is_incremental() %}
WHERE TRY_TO_TIMESTAMP(t.ticket_date) > (SELECT MAX(ticket_date) FROM {{ this }})
{% endif %}
