SELECT
    merch,

    count() AS transaction_count,
    countIf(target = 1) AS fraud_count,

    round(
        100.0 * countIf(target = 1) / count(),
        4
    ) AS fraud_rate,

    round(sum(amount), 2) AS total_amount,
    round(sumIf(amount, target = 1), 2) AS fraud_amount,
    round(avg(amount), 2) AS avg_amount,

    uniqExact(tuple(name_1, name_2, lat, lon))
        AS unique_customers,

    min(transaction_time) AS first_transaction,
    max(transaction_time) AS last_transaction,

    if(
        count() >= 20
        AND 100.0 * countIf(target = 1) / count() >= 5,
        1,
        0
    ) AS is_suspicious

FROM {{ ref('stg_transactions') }}

GROUP BY merch
