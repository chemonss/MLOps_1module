SELECT customer_id, first_transaction, last_transaction
FROM {{ ref('mart_customer_risk_profile') }}
WHERE first_transaction > last_transaction
