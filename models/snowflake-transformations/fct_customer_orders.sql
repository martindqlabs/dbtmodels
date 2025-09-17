-- {{ config(materialized='view') }}

SELECT
    order_item_id,
    order_id,
    customer_id,
    product_name,
    quantity,
    price_per_item,
    order_date,
    order_status,
    total_amount,
    payment_id,
    payment_date,
    quantity * price_per_item AS calculated_total_item_amount,
    CASE 
        WHEN total_amount != quantity * price_per_item THEN 'Mismatch' 
        ELSE 'Match' 
    END AS amount_validation_flag
FROM {{ ref('order_payments') }}
