SELECT
    transaction_date,
    us_state,

    count() AS transaction_count,
    round(sum(amount), 2) AS total_amount,
    round(avg(amount), 2) AS avg_amount,
    round(quantileExact(0.95)(amount), 2) AS p95_amount,

    countIf(amount_bucket IN ('HIGH', 'VERY_HIGH')) AS large_transaction_count,

    round(
        100.0 * countIf(amount_bucket IN ('HIGH', 'VERY_HIGH')) / count(),
        2
    ) AS large_transaction_share_pct

FROM {{ ref('stg_transactions') }}

GROUP BY
    transaction_date,
    us_state
