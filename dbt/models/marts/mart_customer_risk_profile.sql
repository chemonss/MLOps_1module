SELECT
    toString(cityHash64(name_1, name_2, lat, lon)) AS customer_id,

    name_1,
    name_2,

    count() AS transaction_count,
    countIf(target = 1) AS fraud_count,

    round(
        100.0 * countIf(target = 1) / count(),
        4
    ) AS fraud_rate,

    round(sum(amount), 2) AS total_amount,
    round(avg(amount), 2) AS avg_amount,

    min(transaction_time) AS first_transaction,
    max(transaction_time) AS last_transaction,

    CASE
        WHEN 100.0 * countIf(target = 1) / count() >= 5
            THEN 'HIGH'
        WHEN 100.0 * countIf(target = 1) / count() >= 1
            THEN 'MEDIUM'
        ELSE 'LOW'
    END AS risk_level

FROM {{ ref('stg_transactions') }}

GROUP BY
    name_1,
    name_2,
    lat,
    lon
