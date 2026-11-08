SELECT
    day_of_week,
    transaction_hour,

    count() AS transaction_count,
    countIf(target = 1) AS fraud_count,

    round(
        100.0 * countIf(target = 1) / count(),
        4
    ) AS fraud_rate,

    round(sum(amount), 2) AS total_amount,
    round(sumIf(amount, target = 1), 2) AS fraud_amount

FROM {{ ref('stg_transactions') }}

GROUP BY
    day_of_week,
    transaction_hour
