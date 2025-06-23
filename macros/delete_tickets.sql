{% macro delete_tickets() %}
    DELETE FROM {{ ref('insert_customer_tickets') }}
    WHERE ticket_id IN (
        SELECT ticket_id FROM {{ ref('tickets_to_delete') }}
    );
{% endmacro %}