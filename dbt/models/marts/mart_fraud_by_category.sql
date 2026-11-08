SELECT
    cat_id,

    count() AS transaction_count,
    countIf(target = 1) AS fraud_count,

    round(
        100.0 * countIf(target = 1) / count(),
        4
    ) AS fraud_rate,

    round(sum(amount), 2) AS total_amount,
    round(sumIf(amount, target = 1), 2) AS fraud_amount,
    round(avg(amount), 2) AS avg_amount

FROM {{ ref('stg_transactions') }}

GROUP BY cat_id
