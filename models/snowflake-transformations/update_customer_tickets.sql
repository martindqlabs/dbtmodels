{{
    config(
        materialized='incremental',
        unique_key='ticket_id',
        schema='REPORTING',
    )
}}

SELECT 
    t.ticket_id,
    t.customer_id,
    t.issue_type,
    t.description,
    TRY_TO_TIMESTAMP(t.ticket_date) AS ticket_date,

    -- Change null resolution_status to 'resolved'
    COALESCE(t.resolution_status, 'resolved') AS resolution_status,

    t.first_name,
    t.last_name,
    t.email,

    -- Remove dashes from phone numbers
    REPLACE(t.phone_number, '-', '') AS phone_number,

    t.join_date,
    t.status,

    -- Cap loyalty points at 10,000
    CASE 
        WHEN t.loyalty_points > 10000 THEN 10000
        ELSE t.loyalty_points
    END AS loyalty_points

FROM {{ ref('customer_tickets') }} t

{% if is_incremental() %}
WHERE TRY_TO_TIMESTAMP(t.ticket_date) > (SELECT MAX(ticket_date) FROM {{ this }})
{% endif %}
