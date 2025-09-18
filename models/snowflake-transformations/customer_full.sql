SELECT
    customerid AS customer_id,
    firstname AS first_name,
    lastname AS last_name,
    email,
    phonenumber AS phone_number,
    joindate AS join_date,
    status,
    loyaltypoints AS loyalty_points,
    addressid AS address_idD,
    street
FROM DQLABS_QA.staging.customer_full
