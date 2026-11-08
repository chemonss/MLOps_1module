SELECT
    us_state,

    count() AS transaction_count,
    countIf(target = 1) AS fraud_count,

    round(
        100.0 * countIf(target = 1) / count(),
        4
    ) AS fraud_rate,

    uniqExact(tuple(name_1, name_2, lat, lon)) AS unique_customers,
    uniqExact(merch) AS unique_merchants,

    round(sum(amount), 2) AS total_amount,
    round(sumIf(amount, target = 1), 2) AS fraud_amount

FROM {{ ref('stg_transactions') }}

GROUP BY us_state
