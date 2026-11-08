WITH metrics AS (
    SELECT 'category' AS mart, toString(cat_id) AS dimension_key,
           transaction_count, fraud_count
    FROM {{ ref('mart_fraud_by_category') }}
    UNION ALL
    SELECT 'state', toString(us_state), transaction_count, fraud_count
    FROM {{ ref('mart_fraud_by_state') }}
    UNION ALL
    SELECT 'customer', toString(customer_id), transaction_count, fraud_count
    FROM {{ ref('mart_customer_risk_profile') }}
    UNION ALL
    SELECT 'hourly', concat(toString(day_of_week), '-', toString(transaction_hour)), transaction_count, fraud_count
    FROM {{ ref('mart_hourly_fraud_pattern') }}
    UNION ALL
    SELECT 'merchant', toString(merch), transaction_count, fraud_count
    FROM {{ ref('mart_merchant_analytics') }}
)
SELECT *
FROM metrics
WHERE fraud_count > transaction_count OR transaction_count = 0
