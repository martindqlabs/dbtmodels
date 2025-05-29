SELECT
    ticket_id
FROM {{ ref('customer_tickets') }}
WHERE resolution_status = 'duplicate'